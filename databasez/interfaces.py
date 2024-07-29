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


class TransactionBackend(ABC):
    connection: ConnectionBackend

    def __init__(self, connection: ConnectionBackend):
        self.connection = connection

    @abstractmethod
    async def start(self, is_root: bool, extra_options: typing.Dict[str, typing.Any]) -> None: ...

    @abstractmethod
    async def commit(self) -> None: ...

    @abstractmethod
    async def rollback(self) -> None: ...


class ConnectionBackend(ABC):
    database: DatabaseBackend
    async_connection: typing.Optional[typing.Any] = None

    def __init__(self, database: DatabaseBackend):
        self.database = database

    @abstractmethod
    async def get_raw_connection(self) -> typing.Any:
        """
        Get underlying connection of async_connection.
        In sqlalchemy based drivers async_connection is the sqlalchemy handle.
        """

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
        yield True  # type: ignore

    async def iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> AsyncGenerator[Record, None]:
        async for batch in self.batched_iterate(query, batch_size):
            for record in batch:
                yield record

    @abstractmethod
    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> typing.Optional[Record]: ...

    async def fetch_val(
        self, query: ClauseElement, column: typing.Any = 0, pos: int = 0
    ) -> typing.Any:
        row = await self.fetch_one(query, pos=pos)
        if row is None:
            return None
        if isinstance(column, int):
            return row[column]
        return getattr(row, column)

    @abstractmethod
    async def execute_raw(self, stmt: typing.Any) -> typing.Any: ...

    async def execute(self, stmt: typing.Any) -> int:
        """
        Executes statement and returns the last row id (query) or the row count of updates.

        Warning: can return -1 (e.g. psycopg) in case the result is unknown

        """
        with await self.execute_raw(stmt) as result:
            try:
                return typing.cast(int, result.lastrowid)
            except AttributeError:
                return typing.cast(int, result.rowcount)

    @abstractmethod
    async def execute_many(self, stmts: typing.List[typing.Any]) -> None: ...

    @abstractmethod
    def in_transaction(self) -> bool:
        """Is a transaction active?"""

    @abstractmethod
    async def commit(self) -> None:
        """Sometimes connections are wrapped in a transaction."""

    @abstractmethod
    async def rollback(self) -> None:
        """Sometimes connections are wrapped in a transaction."""

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
    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        """
        Start the database backend.

        Note: database_url and options are expected to be sanitized already.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Stop the database backend.
        """

    @abstractmethod
    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple[DatabaseURL, typing.Dict[str, typing.Any]]:
        """
        Extract options from query.

        Return reformated query for creating engine+options.
        """

    def connection(self) -> ConnectionBackend:
        return self.connection_class(database=self)
