import asyncio
import inspect
import typing
from functools import partial

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
