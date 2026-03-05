from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from functools import partial, wraps
from threading import Thread
from typing import Any, TypeVar, cast

from sqlalchemy.engine import Connection as SQLAConnection
from sqlalchemy.engine import Dialect

DATABASEZ_OVERWRITE_LOGGING: bool = False
"""When ``True``, include full tracebacks when overwrite import fails."""

DATABASEZ_RESULT_TIMEOUT: float | None = None
"""Global timeout (seconds) for cross-loop result polling. ``None`` = no limit."""

# Poll with 0.1 ms so the CPU isn't pegged at 100 %.
DATABASEZ_POLL_INTERVAL: float = 0.0001
"""Default poll interval (seconds) for cross-loop awaiting."""


async def _arun_coroutine_threadsafe_result_shim(
    future: Future[Any], loop: asyncio.AbstractEventLoop, poll_interval: float
) -> Any:
    """Poll *future* until it completes, sleeping between checks.

    Args:
        future: A :class:`~concurrent.futures.Future` submitted to *loop*.
        loop: The event loop *future* is running on.
        poll_interval: Seconds to sleep between polls.

    Returns:
        Any: The result of the future.

    Raises:
        RuntimeError: If *loop* is closed before the future finishes.
    """
    while not future.done():
        if loop.is_closed():
            raise RuntimeError("loop submitted to is closed")
        await asyncio.sleep(poll_interval)
    return future.result(0)


async def arun_coroutine_threadsafe(
    coro: Coroutine[Any, Any, Any],
    loop: asyncio.AbstractEventLoop | None,
    poll_interval: float,
) -> Any:
    """Schedule *coro* on a foreign event loop and await the result.

    If the *running* loop is the same as *loop*, the coroutine is simply
    awaited directly.  Otherwise it is submitted via
    :func:`asyncio.run_coroutine_threadsafe` and polled.

    Args:
        coro: The coroutine to run.
        loop: The target event loop (must be running).
        poll_interval: Seconds to sleep between result polls.  A negative
            value disables polling and raises :class:`RuntimeError`.

    Returns:
        Any: The return value of *coro*.

    Raises:
        AssertionError: If *loop* is ``None`` or not running.
        RuntimeError: If *poll_interval* is negative.
        TimeoutError: If :data:`DATABASEZ_RESULT_TIMEOUT` is exceeded.
    """
    running_loop = asyncio.get_running_loop()
    assert loop is not None and loop.is_running(), "loop is closed"
    if running_loop is loop:
        return await coro
    if poll_interval < 0:
        raise RuntimeError("Not supposed to run in the poll path")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return await asyncio.wait_for(
            _arun_coroutine_threadsafe_result_shim(future, loop, poll_interval),
            DATABASEZ_RESULT_TIMEOUT,
        )
    except TimeoutError as exc:
        future.cancel()
        raise exc


async_wrapper_slots = (
    "_async_wrapped",
    "_async_pool",
    "_async_exclude_attrs",
    "_async_exclude_types",
    "_async_stringify_exceptions",
)
default_exclude_types = (int, bool, dict, list, tuple, BaseException)


class AsyncWrapper:
    """Wrap a synchronous DBAPI2 object to present an async interface.

    All method calls on the wrapped object are dispatched to a thread-pool
    executor so that they do not block the event loop.  Certain attributes
    and types can be excluded from async wrapping.

    Attributes:
        _async_wrapped: The original synchronous object.
        _async_pool: The :class:`~concurrent.futures.Executor` used for
            offloading blocking calls.
        _async_exclude_attrs: A mapping of attribute names that should be
            dispatched synchronously or recursively wrapped.
        _async_exclude_types: Tuple of types whose instances are returned
            directly without wrapping.
        _async_stringify_exceptions: When ``True``, re-raise driver
            exceptions as plain :class:`Exception` with stringified messages
            to avoid pickling issues across threads.
    """

    __slots__ = async_wrapper_slots
    _async_wrapped: Any
    _async_pool: Any
    _async_exclude_attrs: dict[str, Any]
    _async_exclude_types: tuple[type[Any], ...]
    _async_stringify_exceptions: bool

    def __init__(
        self,
        wrapped: Any,
        pool: Any,
        exclude_attrs: dict[str, Any] | None = None,
        exclude_types: tuple[type[Any], ...] = default_exclude_types,
        stringify_exceptions: bool = False,
    ) -> None:
        """Initialise the async wrapper.

        Args:
            wrapped: The synchronous object to wrap.
            pool: A :class:`~concurrent.futures.Executor` for blocking calls.
            exclude_attrs: Mapping of attribute names to either ``True``
                (dispatch synchronously) or a nested dict for recursive
                exclusion.
            exclude_types: Types that should be returned un-wrapped.
            stringify_exceptions: Whether to stringify driver exceptions.
        """
        self._async_wrapped = wrapped
        self._async_pool = pool
        self._async_exclude_attrs = exclude_attrs or {}
        self._async_exclude_types = exclude_types
        self._async_stringify_exceptions = stringify_exceptions

    def __getattribute__(self, name: str) -> Any:  # noqa: C901
        """Intercept attribute access and wrap callables for async dispatch.

        Async dunder names (``__aenter__``, ``__aexit__``, ``__aiter__``,
        ``__anext__``) are mapped to their synchronous counterparts on the
        wrapped object.

        Args:
            name: The attribute name being accessed.

        Returns:
            Any: The attribute value — either the original value (for
                excluded attrs / non-callables) or an async wrapper function.
        """
        if name in async_wrapper_slots:
            return super().__getattribute__(name)
        if name == "__aenter__":
            name = "__enter__"
        elif name == "__aexit__":
            name = "__exit__"
        elif name == "__aiter__":
            name = "__iter__"
        elif name == "__anext__":
            name = "__next__"
        try:
            attr = self._async_wrapped.__getattribute__(name)
        except AttributeError:
            if name == "__enter__":

                async def fn() -> Any:
                    return self

                return fn
            elif name == "__exit__":
                attr = self._async_wrapped.__getattribute__("close")
        except Exception as exc:
            if self._async_stringify_exceptions:
                raise Exception(str(exc)) from None
            raise

        if self._async_exclude_attrs.get(name) is True:
            if inspect.isroutine(attr):
                # Submit to threadpool synchronously and wrap result.
                def fn2(*args: Any, **kwargs: Any) -> Any:
                    try:
                        return AsyncWrapper(
                            self._async_pool.submit(partial(attr, *args, **kwargs)).result(),
                            pool=self._async_pool,
                            exclude_attrs={},
                            exclude_types=self._async_exclude_types,
                            stringify_exceptions=self._async_stringify_exceptions,
                        )
                    except Exception as exc:
                        if self._async_stringify_exceptions:
                            raise Exception(str(exc)) from None
                        raise

                return fn2

            return attr
        if not callable(attr):
            return attr
        if isinstance(attr, type) and issubclass(attr, self._async_exclude_types):
            return attr

        async def fn3(*args: Any, **kwargs: Any) -> Any:
            loop = asyncio.get_running_loop()
            try:
                result = await loop.run_in_executor(
                    self._async_pool, partial(attr, *args, **kwargs)
                )
            except Exception as exc:
                if self._async_stringify_exceptions:
                    raise Exception(str(exc)) from None
                raise
            if isinstance(result, self._async_exclude_types):
                return result
            try:
                return AsyncWrapper(
                    result,
                    pool=self._async_pool,
                    exclude_attrs=self._async_exclude_attrs.get(name),
                    exclude_types=self._async_exclude_types,
                    stringify_exceptions=self._async_stringify_exceptions,
                )
            except StopIteration:
                raise StopAsyncIteration from None

        return fn3


class ThreadPassingExceptions(Thread):
    """A :class:`~threading.Thread` that re-raises exceptions on :meth:`join`.

    When the thread's ``run()`` method raises an exception it is captured and
    re-raised in the thread that calls :meth:`join`.
    """

    _exc_raised: BaseException | None = None

    def run(self) -> None:
        """Execute the thread target, capturing any exception."""
        try:
            super().run()
        except Exception as exc:
            self._exc_raised = exc

    def join(self, timeout: float | int | None = None) -> None:
        """Wait for the thread to finish and re-raise captured exceptions.

        Args:
            timeout: Maximum seconds to wait (``None`` = forever).

        Raises:
            Exception: Any exception that occurred inside the thread.
        """
        try:
            super().join(timeout=timeout)
        finally:
            if self._exc_raised is not None:
                raise self._exc_raised


MultiloopProtectorCallable = TypeVar("MultiloopProtectorCallable", bound=Callable)


def _run_with_timeout(inp: Any, timeout: float | None) -> Any:
    """Wrap an awaitable with :func:`asyncio.wait_for` if *timeout* is positive.

    This is a *synchronous* helper that returns a new awaitable (or the
    original value if it is not awaitable / timeout is not set).

    Args:
        inp: A value or awaitable.
        timeout: Maximum seconds, or ``None`` / non-positive to skip.

    Returns:
        Any: The (possibly wrapped) value.
    """
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        inp = asyncio.wait_for(inp, timeout=timeout)
    return inp


async def _arun_with_timeout(inp: Any, timeout: float | None) -> Any:
    """Await *inp* with an optional timeout.

    Args:
        inp: A value or awaitable.
        timeout: Maximum seconds, or ``None`` / non-positive to skip.

    Returns:
        Any: The resolved value.

    Raises:
        TimeoutError: If the timeout is exceeded.
    """
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        return await asyncio.wait_for(inp, timeout=timeout)
    elif inspect.isawaitable(inp):
        return await inp
    return inp


def multiloop_protector(
    fail_with_different_loop: bool,
    inject_parent: bool = False,
    passthrough_timeout: bool = False,
) -> Callable[[MultiloopProtectorCallable], MultiloopProtectorCallable]:
    """Decorator that guards methods against cross-event-loop calls.

    When the *current* running loop differs from the object's ``_loop``, the
    decorator either:

    - **Raises** :class:`RuntimeError` (if *fail_with_different_loop* is
      ``True``), or
    - **Transparently proxies** the call through the object's
      ``async_helper`` (for seamless multi-loop / multi-thread support).

    If the calling loop matches, the decorated function is invoked directly,
    optionally wrapped with a timeout via :func:`_run_with_timeout`.

    Args:
        fail_with_different_loop: If ``True``, raise instead of proxying.
        inject_parent: If ``True``, inject a ``parent_database`` keyword
            argument when redirecting to a sub-database.
        passthrough_timeout: If ``True``, do **not** pop ``timeout`` from
            kwargs (some callers handle it themselves).

    Returns:
        Callable: A method decorator.
    """

    def _decorator(fn: MultiloopProtectorCallable) -> MultiloopProtectorCallable:
        @wraps(fn)
        def wrapper(
            self: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            timeout: float | None = None
            if not passthrough_timeout and "timeout" in kwargs:
                timeout = kwargs.pop("timeout")
            if inject_parent:
                assert "parent_database" not in kwargs, '"parent_database" is a reserved keyword'
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None and self._loop is not None and loop != self._loop:
                # Redirect call if self is Database and loop is in sub-databases.
                if hasattr(self, "_databases_map") and loop in self._databases_map:
                    if inject_parent:
                        kwargs["parent_database"] = self
                    self = self._databases_map[loop]
                else:
                    if fail_with_different_loop:
                        raise RuntimeError("Different loop used")
                    return self.async_helper(self, fn, args, kwargs, timeout=timeout)
            return _run_with_timeout(fn(self, *args, **kwargs), timeout=timeout)

        return cast(MultiloopProtectorCallable, wrapper)

    return _decorator


def get_dialect(async_conn_or_dialect: SQLAConnection | Dialect, /) -> Dialect:
    """Extract the :class:`~sqlalchemy.engine.Dialect` from a connection.

    Args:
        async_conn_or_dialect: A SQLAlchemy connection or dialect instance.

    Returns:
        Dialect: The resolved dialect.
    """
    if isinstance(async_conn_or_dialect, Dialect):
        return async_conn_or_dialect
    if hasattr(async_conn_or_dialect, "bind"):
        async_conn_or_dialect = async_conn_or_dialect.bind
    return cast("SQLAConnection", async_conn_or_dialect).dialect


def get_quoter(async_conn_or_dialect: SQLAConnection | Dialect, /) -> Callable[[str], str]:
    """Return an identifier-quoting function for the given dialect.

    Args:
        async_conn_or_dialect: A SQLAlchemy connection or dialect instance.

    Returns:
        Callable[[str], str]: A function that quotes a SQL identifier.
    """
    dialect = get_dialect(async_conn_or_dialect)
    if hasattr(dialect, "identifier_preparer"):
        return dialect.identifier_preparer.quote
    return dialect.preparer(dialect).quote
