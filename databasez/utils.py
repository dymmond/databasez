import asyncio
import inspect
import typing
from functools import partial

async_wrapper_slots = (
    "_async_wrapped",
    "_async_pool",
    "_async_exclude_attrs",
    "_async_exclude_types",
)
default_exclude_types = (int, bool, dict, list, tuple)


class AsyncWrapper:
    __slots__ = async_wrapper_slots
    _async_wrapped: typing.Any
    _async_pool: typing.Any
    _async_exclude_attrs: typing.Dict[str, typing.Any]
    _async_exclude_types: typing.Tuple[typing.Any, ...]

    def __init__(self, wrapped, pool, exclude_attrs=None, exclude_types=default_exclude_types):
        self._async_wrapped = wrapped
        self._async_pool = pool
        self._async_exclude_attrs = exclude_attrs or {}
        self._async_exclude_types = exclude_types

    def __getattribute__(self, name: str) -> typing.Any:
        if name in async_wrapper_slots or self._async_exclude_attrs.get(name) is True:
            return super().__getattribute__(name)
        if name == "__aenter__":
            name = "__enter__"
        elif name == "__aexit__":
            name = "__exit__"
        elif name == "__aiter__":
            name = "__iter__"
        elif name == "__anext__":
            name = "__next__"
        attr = self._async_wrapped.__getattribute__(name)
        if inspect.ismethod(attr):

            async def fn(*args, **kwargs):
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self._async_pool, partial(attr, *args, **kwargs)
                )
                if isinstance(result, self._async_exclude_types):
                    return result
                try:
                    return AsyncWrapper(
                        result,
                        pool=self._async_pool,
                        exclude_attrs=self._async_exclude_attrs.get(name),
                        exclude_types=self._async_exclude_types,
                    )
                except StopIteration:
                    raise StopAsyncIteration from None

            return fn

        return attr
