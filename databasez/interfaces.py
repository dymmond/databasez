from __future__ import annotations

__all__ = ["DatabaseBackend", "ConnectionBackend", "TransactionBackend"]


import typing
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping, Sequence

if typing.TYPE_CHECKING:
    from sqlalchemy import AsyncEngine
    from sqlalchemy.sql import ClauseElement

    from databasez.core.databaseurl import DatabaseURL


class Record(Sequence):
    @property
    def _mapping(self) -> Mapping[str, typing.Any]:
        raise NotImplementedError()  # pragma: no cover

    def __getitem__(self, key: int) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend(ABC):
    connection: ConnectionBackend
    is_root: bool = False

    def __init__(self, connection: ConnectionBackend):
        self.connection = connection

    @abstractmethod
    async def start(self, is_root: bool, extra_options: typing.Dict[str, typing.Any]) -> None: ...

    @abstractmethod
    async def commit(self) -> None: ...

    @abstractmethod
    async def rollback(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...


class ConnectionBackend(ABC):
    database: DatabaseBackend
    raw_connection: typing.Optional[typing.Any] = None

    def __init__(self, database: DatabaseBackend):
        self.database = database

    @abstractmethod
    async def get_raw_connection(self) -> typing.Any:
        """Get underlying connection."""

    @abstractmethod
    async def acquire(self) -> None: ...

    @abstractmethod
    async def release(self) -> None: ...

    @abstractmethod
    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]: ...

    @abstractmethod
    async def batched_iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> AsyncGenerator[typing.Sequence[Record], None]:
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True

    async def iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> AsyncGenerator[Record, None]:
        async for batch in self.batched_iterate(query, batch_size):
            for record in batch:
                yield record

    @abstractmethod
    async def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]: ...

    async def fetch_val(self, query: ClauseElement, column: typing.Any = 0) -> typing.Any:
        row = await self.fetch_one(query)
        if row is None:
            return None
        if isinstance(column, int):
            return row[column]
        return getattr(row, column)

    @abstractmethod
    async def execute_raw(self, stmt: typing.Any) -> typing.Any: ...

    async def execute(self, stmt: typing.Any) -> int:
        with await self.execute_raw(stmt) as result:
            try:
                return result.lastrowid
            except AttributeError:
                return

    @abstractmethod
    async def execute_many(self, stmts: typing.List[typing.Any]) -> None: ...

    def transaction(self) -> TransactionBackend:
        return self.database.transaction_class(self)

    @property
    def engine(self) -> typing.Optional[AsyncEngine]:
        return self.database.engine


class DatabaseBackend(ABC):
    engine: typing.Optional[AsyncEngine] = None
    connection_class: typing.Type[ConnectionBackend]
    transaction_class: typing.Type[TransactionBackend]

    def __init__(
        self,
        connection_class: typing.Type[ConnectionBackend],
        transaction_class: typing.Type[TransactionBackend],
    ):
        self.connection_class = connection_class
        self.transaction_class = transaction_class

    @abstractmethod
    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    def reformat_query(self, database_url: DatabaseURL) -> DatabaseURL:
        """Reformat query options"""
        return database_url

    def connection(self) -> ConnectionBackend:
        return self.connection_class(database=self)
