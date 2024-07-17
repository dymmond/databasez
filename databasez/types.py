from collections.abc import Sequence
from typing import Any, Callable, Dict, TypeVar

from databasez.interfaces import Record as RecordInterface

DictAny = Dict[str, Any]

BatchCallableResult = TypeVar("BatchCallableResult")
BatchCallable = Callable[[Sequence[RecordInterface]], BatchCallableResult]
