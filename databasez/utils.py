import asyncio
import inspect
import typing
from functools import partial, wraps
from threading import Thread
from types import TracebackType

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
    _async_wrapped: typing.Any
    _async_pool: typing.Any
    _async_exclude_attrs: typing.Dict[str, typing.Any]
    _async_exclude_types: typing.Tuple[typing.Type[typing.Any], ...]
    _async_stringify_exceptions: bool

    def __init__(
        self,
        wrapped: typing.Any,
        pool: typing.Any,
        exclude_attrs: typing.Optional[typing.Dict[str, typing.Any]] = None,
        exclude_types: typing.Tuple[typing.Type[typing.Any], ...] = default_exclude_types,
        stringify_exceptions: bool = False,
    ) -> None:
        self._async_wrapped = wrapped
        self._async_pool = pool
        self._async_exclude_attrs = exclude_attrs or {}
        self._async_exclude_types = exclude_types
        self._async_stringify_exceptions = stringify_exceptions

    def __getattribute__(self, name: str) -> typing.Any:
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

                async def fn() -> typing.Any:
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
                def fn2(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
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

        async def fn3(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
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
    _exc_raised: typing.Any = None

    def run(self) -> None:
        try:
            super().run()
        except Exception as exc:
            self._exc_raised = exc

    def join(self, timeout: typing.Union[float, int, None] = None) -> None:
        super().join(timeout=timeout)
        if self._exc_raised:
            raise self._exc_raised


MultiloopProtectorCallable = typing.TypeVar("MultiloopProtectorCallable", bound=typing.Callable)


def _run_with_timeout(inp: typing.Any, timeout: typing.Optional[float]) -> typing.Any:
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        inp = asyncio.wait_for(inp, timeout=timeout)
    return inp


async def _arun_with_timeout(inp: typing.Any, timeout: typing.Optional[float]) -> typing.Any:
    if timeout is not None and timeout > 0 and inspect.isawaitable(inp):
        inp = await asyncio.wait_for(inp, timeout=timeout)
    elif inspect.isawaitable(inp):
        return await inp
    return inp


class AsyncHelperDatabase:
    def __init__(
        self,
        database: typing.Any,
        fn: typing.Callable,
        args: typing.Any,
        kwargs: typing.Any,
        timeout: typing.Optional[float],
    ) -> None:
        self.database = database
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> typing.Any:
        async with self.database as database:
            return await _arun_with_timeout(
                self.fn(database, *self.args, **self.kwargs), self.timeout
            )

    def __await__(self) -> typing.Any:
        return self.call().__await__()

    async def __aenter__(self) -> typing.Any:
        database = await self.database.__aenter__()
        self.ctm = await _arun_with_timeout(
            self.fn(database, *self.args, **self.kwargs), timeout=self.timeout
        )
        return await self.ctm.__aenter__()

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        assert self.ctm is not None
        try:
            await _arun_with_timeout(self.ctm.__aexit__(exc_type, exc_value, traceback), None)
        finally:
            await self.database.__aexit__()


class AsyncHelperConnection:
    def __init__(
        self,
        connection: typing.Any,
        fn: typing.Callable,
        args: typing.Any,
        kwargs: typing.Any,
        timeout: typing.Optional[float],
    ) -> None:
        self.connection = connection
        self.fn = partial(fn, self.connection, *args, **kwargs)
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> typing.Any:
        async with self.connection:
            # is automatically awaited
            result = await _arun_with_timeout(self.fn(), self.timeout)
            return result

    async def acall(self) -> typing.Any:
        return asyncio.run_coroutine_threadsafe(self.call(), self.connection._loop).result()

    def __await__(self) -> typing.Any:
        return self.acall().__await__()

    async def enter_intern(self) -> typing.Any:
        await self.connection.__aenter__()
        self.ctm = await self.call()
        return await self.ctm.__aenter__()

    async def exit_intern(self) -> typing.Any:
        assert self.ctm is not None
        try:
            await self.ctm.__aexit__()
        finally:
            self.ctm = None
            await self.connection.__aexit__()

    async def __aenter__(self) -> typing.Any:
        return asyncio.run_coroutine_threadsafe(
            self.enter_intern(), self.connection._loop
        ).result()

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        assert self.ctm is not None
        asyncio.run_coroutine_threadsafe(self.exit_intern(), self.connection._loop).result()


def multiloop_protector(
    fail_with_different_loop: bool, inject_parent: bool = False, passthrough_timeout: bool = False
) -> typing.Callable[[MultiloopProtectorCallable], MultiloopProtectorCallable]:
    """For multiple threads or other reasons why the loop changes"""

    # True works with all methods False only for methods of Database
    # needs _loop attribute to check against
    def _decorator(fn: MultiloopProtectorCallable) -> MultiloopProtectorCallable:
        @wraps(fn)
        def wrapper(
            self: typing.Any,
            *args: typing.Any,
            **kwargs: typing.Any,
        ) -> typing.Any:
            timeout: typing.Optional[float] = None
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
                    helper = (
                        AsyncHelperDatabase
                        if hasattr(self, "_databases_map")
                        else AsyncHelperConnection
                    )
                    return helper(self, fn, args, kwargs, timeout=timeout)
            return _run_with_timeout(fn(self, *args, **kwargs), timeout=timeout)

        return typing.cast(MultiloopProtectorCallable, wrapper)

    return _decorator
