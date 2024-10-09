from __future__ import annotations

__all__ = ["Record", "DatabaseBackend", "ConnectionBackend", "TransactionBackend"]


import weakref
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlalchemy import AsyncEngine, Transaction
    from sqlalchemy.sql import ClauseElement

    from databasez.core.database import Connection as RootConnection
    from databasez.core.database import Database as RootDatabase
    from databasez.core.databaseurl import DatabaseURL
    from databasez.core.transaction import Transaction as RootTransaction


class Record(Sequence):
    @property
    def _mapping(self) -> Mapping[str, Any]:
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend(ABC):
    raw_transaction: Transaction | None

    def __init__(
        self,
        connection: ConnectionBackend,
        existing_transaction: Transaction | None = None,
    ):
        # cannot be a weak ref otherwise connections get lost when retrieving them via transactions
        self.connection = connection
        self.raw_transaction = existing_transaction

    @property
    def connection(self) -> ConnectionBackend | None:
        result = self.__dict__.get("connection")
        if result is None:
            return None
        return cast(ConnectionBackend, result())

    @connection.setter
    def connection(self, value: ConnectionBackend) -> None:
        self.__dict__["connection"] = weakref.ref(value)

    @property
    def async_connection(self) -> Any | None:
        result = self.connection
        if result is None:
            return None
        return result.async_connection

    @property
    def owner(self) -> RootTransaction | None:
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootTransaction", result())

    @owner.setter
    def owner(self, value: RootTransaction) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def start(self, is_root: bool, **extra_options: dict[str, Any]) -> None: ...

    @abstractmethod
    async def commit(self) -> None: ...

    @abstractmethod
    async def rollback(self) -> None: ...

    @abstractmethod
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: dict[str, Any]
    ) -> str | None: ...

    @property
    def database(self) -> DatabaseBackend | None:
        conn = self.connection
        if conn is None:
            return None
        return conn.database

    @property
    def engine(self) -> AsyncEngine | None:
        database = self.database
        if database is None:
            return None
        return database.engine

    @property
    def root(self) -> RootDatabase | None:
        database = self.database
        if database is None:
            return None
        return database.owner


class ConnectionBackend(ABC):
    async_connection: Any | None = None

    def __init__(self, database: DatabaseBackend):
        self.database = database

    @property
    def database(self) -> DatabaseBackend | None:
        result = self.__dict__.get("database")
        if result is None:
            return None
        return cast(DatabaseBackend, result())

    @database.setter
    def database(self, value: DatabaseBackend) -> None:
        self.__dict__["database"] = weakref.ref(value)

    @property
    def owner(self) -> RootConnection | None:
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootConnection", result())

    @owner.setter
    def owner(self, value: RootConnection) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def get_raw_connection(self) -> Any:
        """
        Get underlying connection of async_connection.
        In sqlalchemy based drivers async_connection is the sqlalchemy handle.
        """

    @abstractmethod
    async def acquire(self) -> Any | None: ...

    @abstractmethod
    async def release(self) -> None: ...

    @abstractmethod
    async def fetch_all(self, query: ClauseElement) -> list[Record]: ...

    @abstractmethod
    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Sequence[Record], None]:
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True  # type: ignore

    async def iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Record, None]:
        async for batch in self.batched_iterate(query, batch_size):
            for record in batch:
                yield record

    @abstractmethod
    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> Record | None: ...

    async def fetch_val(self, query: ClauseElement, column: Any = 0, pos: int = 0) -> Any:
        row = await self.fetch_one(query, pos=pos)
        if row is None:
            return None
        if isinstance(column, int):
            return row[column]
        return getattr(row, column)

    @abstractmethod
    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...

    @abstractmethod
    async def execute_raw(self, stmt: Any, value: Any = None) -> Any: ...

    @abstractmethod
    async def execute(self, stmt: Any, value: Any = None) -> Record | int:
        """
        Executes statement and returns the last row defaults (insert) or rowid (insert) or the row count of updates.
        """

    @abstractmethod
    async def execute_many(self, stmt: Any, value: Any = None) -> Sequence[Record] | int:
        """
        Executes statement and returns the row defaults (insert) or the row count of operations.
        """

    @abstractmethod
    def in_transaction(self) -> bool:
        """Is a transaction active?"""

    def transaction(self, existing_transaction: Any | None = None) -> TransactionBackend:
        database = self.database
        assert database is not None
        return database.transaction_class(self, existing_transaction)

    @property
    def engine(self) -> AsyncEngine | None:
        database = self.database
        if database is None:
            return None
        return database.engine


class DatabaseBackend(ABC):
    engine: AsyncEngine | None = None
    connection_class: type[ConnectionBackend]
    transaction_class: type[TransactionBackend]
    default_batch_size: int

    def __init__(
        self,
        connection_class: type[ConnectionBackend],
        transaction_class: type[TransactionBackend],
    ):
        self.connection_class = connection_class
        self.transaction_class = transaction_class

    def __copy__(self) -> DatabaseBackend:
        return self.__class__(self.connection_class, self.transaction_class)

    @property
    def owner(self) -> RootDatabase | None:
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootDatabase", result())

    @owner.setter
    def owner(self, value: RootDatabase) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        """
        Set root and start the database backend.

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
        **options: dict[str, Any],
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """
        Extract options from query.

        Return reformated query for creating engine+options.
        """

    def connection(self) -> ConnectionBackend:
        return self.connection_class(database=self)
