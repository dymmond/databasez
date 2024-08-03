from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Callable, Dict, TypeVar

if TYPE_CHECKING:
    from databasez import interfaces

DictAny = Dict[str, Any]

BatchCallableResult = TypeVar("BatchCallableResult")
BatchCallable = Callable[[Sequence["interfaces.Record"]], BatchCallableResult]
