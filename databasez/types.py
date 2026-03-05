from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from databasez import interfaces

DictAny = dict[str, Any]
"""Generic dictionary with string keys and arbitrary values."""

BatchCallableResult = TypeVar("BatchCallableResult")
"""Type variable representing the return type of a batch callable."""

BatchCallable = Callable[[Sequence["interfaces.Record"]], BatchCallableResult]
"""Callable that transforms a sequence of records into a batch result."""
