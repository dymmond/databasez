from __future__ import annotations

__all__ = ["DatabaseBackend", "ConnectionBackend", "TransactionBackend"]


import typing
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping, Sequence

from sqlalchemy.sql import ClauseElement


class Record(Sequence):
    @property
    def _mapping(self) -> Mapping:
        raise NotImplementedError()  # pragma: no cover

    def __getitem__(self, key: typing.Any) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend(ABC):
    @abstractmethod
    async def start(self, is_root: bool, extra_options: typing.Dict[str, typing.Any]) -> None: ...

    @abstractmethod
    async def commit(self) -> None: ...

    @abstractmethod
    async def rollback(self) -> None: ...


class ConnectionBackend(ABC):
    @abstractmethod
    async def acquire(self) -> None: ...

    @abstractmethod
    async def release(self) -> None: ...

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        return [record async for record in self.iterate(query)]

    @abstractmethod
    async def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]: ...

    async def fetch_val(self, query: ClauseElement, column: typing.Any = 0) -> typing.Any:
        row = await self.fetch_one(query)
        return None if row is None else row[column]

    @abstractmethod
    async def execute(self, query: ClauseElement) -> typing.Any: ...

    @abstractmethod
    async def execute_many(self, queries: typing.List[ClauseElement]) -> None: ...

    @abstractmethod
    async def iterate(self, query: ClauseElement) -> AsyncGenerator[Record, None]:
        raise NotImplementedError()  # pragma: no cover
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True  # pragma: no cover

    @abstractmethod
    def transaction(self) -> TransactionBackend: ...

    @property
    @abstractmethod
    def raw_connection(self) -> typing.Any: ...


class DatabaseBackend(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    def connection(self) -> ConnectionBackend: ...
