from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from databasez import interfaces

DictAny = dict[str, Any]

BatchCallableResult = TypeVar("BatchCallableResult")
BatchCallable = Callable[[Sequence["interfaces.Record"]], BatchCallableResult]
