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

DATABASEZ_RESULT_TIMEOUT: float | None = None
# Poll with 0.1ms, this way CPU isn't at 100%
DATABASEZ_POLL_INTERVAL: float = 0.0001


async def _arun_coroutine_threadsafe_result_shim(
    future: Future, loop: asyncio.AbstractEventLoop, poll_interval: float
) -> Any:
    while not future.done():
        if loop.is_closed():
            raise RuntimeError("loop submitted to is closed")
        await asyncio.sleep(poll_interval)
    return future.result(0)


async def arun_coroutine_threadsafe(
    coro: Coroutine, loop: asyncio.AbstractEventLoop | None, poll_interval: float
) -> Any:
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
        self._async_wrapped = wrapped
        self._async_pool = pool
        self._async_exclude_attrs = exclude_attrs or {}
        self._async_exclude_types = exclude_types
        self._async_stringify_exceptions = stringify_exceptions

    def __getattribute__(self, name: str) -> Any:
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
            else:
                raise exc

        if self._async_exclude_attrs.get(name) is True:
            if inspect.isroutine(attr):
                # submit to threadpool
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
                        else:
                            raise exc
                        if self._async_stringify_exceptions:
                            raise Exception(str(exc)) from None
                        else:
                            raise exc

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
                else:
                    raise exc
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
    _exc_raised: Any = None

    def run(self) -> None:
        try:
            super().run()
        except Exception as exc:
            self._exc_raised = exc

    def join(self, timeout: float | int | None = None) -> None:
        try:
            super().join(timeout=timeout)
        finally:
            if self._exc_raised is not None:
                raise self._exc_raised


MultiloopProtectorCallable = TypeVar("MultiloopProtectorCallable", bound=Callable)


def _run_with_timeout(inp: Any, timeout: float | None) -> Any:
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        inp = asyncio.wait_for(inp, timeout=timeout)
    return inp


async def _arun_with_timeout(inp: Any, timeout: float | None) -> Any:
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        return await asyncio.wait_for(inp, timeout=timeout)
    elif inspect.isawaitable(inp):
        return await inp
    return inp


def multiloop_protector(
    fail_with_different_loop: bool, inject_parent: bool = False, passthrough_timeout: bool = False
) -> Callable[[MultiloopProtectorCallable], MultiloopProtectorCallable]:
    """For multiple threads or other reasons why the loop changes"""

    # True works with all methods False only for methods of Database
    # needs _loop attribute to check against
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
                # redirect call if self is Database and loop is in sub databases referenced
                # afaik we can careless continue use the old database object from a subloop and all protected
                # methods are forwarded
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
    if isinstance(async_conn_or_dialect, Dialect):
        return async_conn_or_dialect
    if hasattr(async_conn_or_dialect, "bind"):
        async_conn_or_dialect = async_conn_or_dialect.bind
    return cast("SQLAConnection", async_conn_or_dialect).dialect


def get_quoter(async_conn_or_dialect: SQLAConnection | Dialect, /) -> Callable[[str], str]:
    # needs underlying async connection as object or dialect
    dialect = get_dialect(async_conn_or_dialect)
    if hasattr(dialect, "identifier_preparer"):
        return dialect.identifier_preparer.quote
    return dialect.preparer(dialect).quote
