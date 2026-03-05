from __future__ import annotations

import weakref
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    try:
        from sqlalchemy.engine import Transaction
        from sqlalchemy.ext.asyncio.engine import AsyncEngine
        from sqlalchemy.sql import ClauseElement
    except ImportError:
        AsyncEngine = Any
        Transaction = Any
        ClauseElement = Any
    from databasez.core.database import Connection as RootConnection
    from databasez.core.database import Database as RootDatabase
    from databasez.core.databaseurl import DatabaseURL
    from databasez.core.transaction import Transaction as RootTransaction

__all__ = ["Record", "DatabaseBackend", "ConnectionBackend", "TransactionBackend"]


class Record(Sequence):
    """Base class for database result rows.

    A ``Record`` behaves like an immutable :class:`~collections.abc.Sequence`
    (supporting integer-index access) and exposes a ``_mapping`` property for
    column-name access.

    Concrete implementations are provided by the SQLAlchemy result-proxy layer.
    """

    @property
    def _mapping(self) -> Mapping[str, Any]:
        """Return a mapping view of column names to values.

        Returns:
            Mapping[str, Any]: Column-name to value mapping.

        Raises:
            NotImplementedError: Always — must be overridden by subclasses.
        """
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend(ABC):
    """Abstract backend for managing a single database transaction.

    Attributes:
        raw_transaction: The underlying driver-level transaction handle,
            or ``None`` if the transaction has not yet been started.
    """

    raw_transaction: Transaction | None

    def __init__(
        self,
        connection: ConnectionBackend,
        existing_transaction: Transaction | None = None,
    ) -> None:
        """Initialise the transaction backend.

        Args:
            connection: The connection backend that owns this transaction.
            existing_transaction: An already-started driver-level transaction
                to wrap, or ``None`` to create a new one.
        """
        # Cannot be a true weak-ref otherwise connections get lost when
        # retrieving them via transactions.
        self.connection = connection
        self.raw_transaction = existing_transaction

    @property
    def connection(self) -> ConnectionBackend | None:
        """Return the owning :class:`ConnectionBackend`, or ``None``.

        Returns:
            ConnectionBackend | None: The connection backend, if still alive.
        """
        result = self.__dict__.get("connection")
        if result is None:
            return None
        return cast(ConnectionBackend, result())

    @connection.setter
    def connection(self, value: ConnectionBackend) -> None:
        self.__dict__["connection"] = weakref.ref(value)

    @property
    def async_connection(self) -> Any | None:
        """Return the raw async connection from the owning backend.

        Returns:
            Any | None: The async connection handle, if the backend is alive.
        """
        result = self.connection
        if result is None:
            return None
        return result.async_connection

    @property
    def owner(self) -> RootTransaction | None:
        """Return the high-level :class:`Transaction` wrapper, if alive.

        Returns:
            RootTransaction | None: The wrapper, or ``None``.
        """
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootTransaction", result())

    @owner.setter
    def owner(self, value: RootTransaction) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def start(self, is_root: bool, **extra_options: dict[str, Any]) -> None:
        """Begin the transaction.

        Args:
            is_root: ``True`` when this is the outermost transaction on the
                connection (i.e. not a savepoint).
            **extra_options: Driver-specific transaction options such as
                ``isolation_level``.
        """
        ...  # pragma: no cover

    @abstractmethod
    async def commit(self) -> None:
        """Commit the transaction."""
        ...  # pragma: no cover

    @abstractmethod
    async def rollback(self) -> None:
        """Roll back the transaction."""
        ...  # pragma: no cover

    @abstractmethod
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: dict[str, Any]
    ) -> str | None:
        """Return the default isolation level for new transactions.

        Args:
            is_root: Whether the transaction is the outermost one.
            **extra_options: Additional driver-specific options.

        Returns:
            str | None: The isolation-level string, or ``None`` to keep the
                current level.
        """
        ...  # pragma: no cover

    @property
    def database(self) -> DatabaseBackend | None:
        """Traverse up to the :class:`DatabaseBackend`.

        Returns:
            DatabaseBackend | None: The database backend, or ``None``.
        """
        conn = self.connection
        if conn is None:
            return None
        return conn.database

    @property
    def engine(self) -> AsyncEngine | None:
        """Return the SQLAlchemy :class:`AsyncEngine`, if available.

        Returns:
            AsyncEngine | None: The engine, or ``None``.
        """
        database = self.database
        if database is None:
            return None
        return database.engine

    @property
    def root(self) -> RootDatabase | None:
        """Return the high-level :class:`Database` wrapper.

        Returns:
            RootDatabase | None: The wrapper, or ``None``.
        """
        database = self.database
        if database is None:
            return None
        return database.owner


class ConnectionBackend(ABC):
    """Abstract backend wrapping a single database connection.

    Attributes:
        async_connection: The underlying driver-level async connection,
            or ``None`` when the connection has not yet been acquired.
    """

    async_connection: Any | None = None

    def __init__(self, database: DatabaseBackend) -> None:
        """Initialise the connection backend.

        Args:
            database: The database backend that owns this connection.
        """
        self.database = database

    @property
    def database(self) -> DatabaseBackend | None:
        """Return the owning :class:`DatabaseBackend`, or ``None``.

        Returns:
            DatabaseBackend | None: The database backend, if alive.
        """
        result = self.__dict__.get("database")
        if result is None:
            return None
        return cast(DatabaseBackend, result())

    @database.setter
    def database(self, value: DatabaseBackend) -> None:
        self.__dict__["database"] = weakref.ref(value)

    @property
    def owner(self) -> RootConnection | None:
        """Return the high-level :class:`Connection` wrapper, if alive.

        Returns:
            RootConnection | None: The wrapper, or ``None``.
        """
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootConnection", result())

    @owner.setter
    def owner(self, value: RootConnection) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def get_raw_connection(self) -> Any:
        """Return the underlying driver-level connection.

        In SQLAlchemy-based drivers ``async_connection`` is the SQLAlchemy
        handle; this method returns the *real* driver connection beneath it.

        Returns:
            Any: The raw driver connection.
        """

    @abstractmethod
    async def acquire(self) -> Any | None:
        """Acquire the underlying connection from the pool.

        Returns:
            Any | None: An existing transaction handle if the connection
                was already inside a transaction, or ``None``.
        """
        ...  # pragma: no cover

    @abstractmethod
    async def release(self) -> None:
        """Release the connection back to the pool."""
        ...  # pragma: no cover

    @abstractmethod
    async def fetch_all(self, query: ClauseElement) -> list[Record]:
        """Execute *query* and return all result rows.

        Args:
            query: A compiled SQLAlchemy clause element.

        Returns:
            list[Record]: All result rows.
        """
        ...  # pragma: no cover

    @abstractmethod
    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Sequence[Record], None]:
        """Execute *query* and yield result rows in batches.

        Args:
            query: A compiled SQLAlchemy clause element.
            batch_size: Number of rows per batch.  ``None`` uses the backend
                default.

        Yields:
            Sequence[Record]: A batch of result rows.
        """
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True  # type: ignore

    async def iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Record, None]:
        """Execute *query* and yield result rows one by one.

        The default implementation delegates to :meth:`batched_iterate` and
        flattens the batches.

        Args:
            query: A compiled SQLAlchemy clause element.
            batch_size: Hint for the backend batch size.  ``None`` uses the
                backend default.

        Yields:
            Record: A single result row.
        """
        async for batch in self.batched_iterate(query, batch_size):
            for record in batch:
                yield record

    @abstractmethod
    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> Record | None:
        """Execute *query* and return a single result row.

        Args:
            query: A compiled SQLAlchemy clause element.
            pos: The positional index of the row to return (0-based).  Use
                ``-1`` for the last row.

        Returns:
            Record | None: The requested row, or ``None`` if no rows matched.
        """
        ...  # pragma: no cover

    async def fetch_val(self, query: ClauseElement, column: int | str = 0, pos: int = 0) -> Any:
        """Execute *query* and return a single scalar value.

        Args:
            query: A compiled SQLAlchemy clause element.
            column: Column index (``int``) or column name (``str``).
            pos: Positional index of the row (see :meth:`fetch_one`).

        Returns:
            Any: The scalar value, or ``None`` if no rows matched.
        """
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
    ) -> Any:
        """Run a synchronous callable inside the connection's thread.

        Args:
            fn: A synchronous function to execute.
            *args: Positional arguments forwarded to *fn*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            Any: The return value of *fn*.
        """
        ...  # pragma: no cover

    @abstractmethod
    async def execute_raw(self, stmt: Any, value: Any = None) -> Any:
        """Execute a raw statement and return the driver result proxy.

        Args:
            stmt: The statement to execute.
            value: Optional bind parameters.

        Returns:
            Any: The raw driver result.
        """
        ...  # pragma: no cover

    @abstractmethod
    async def execute(self, stmt: Any, value: Any = None) -> Record | int:
        """Execute a statement and return a concise result.

        For ``INSERT`` statements this returns the inserted primary-key row
        or ``lastrowid``.  For other DML statements it returns ``rowcount``.

        Args:
            stmt: The statement to execute.
            value: Optional bind parameters.

        Returns:
            Record | int: Primary-key row / lastrowid / rowcount.
        """

    @abstractmethod
    async def execute_many(self, stmt: Any, value: Any = None) -> Sequence[Record] | int:
        """Execute a statement with multiple parameter sets.

        For ``INSERT`` statements this returns the inserted primary-key rows
        (when supported by the driver).  Otherwise returns ``rowcount``.

        Args:
            stmt: The statement to execute.
            value: A sequence of parameter mappings.

        Returns:
            Sequence[Record] | int: Primary-key rows / rowcount.
        """

    @abstractmethod
    def in_transaction(self) -> bool:
        """Return whether a transaction is currently active.

        Returns:
            bool: ``True`` if a transaction is active.
        """
        ...  # pragma: no cover

    def transaction(self, existing_transaction: Any | None = None) -> TransactionBackend:
        """Create a new :class:`TransactionBackend` on this connection.

        Args:
            existing_transaction: An existing driver-level transaction to
                wrap, or ``None`` to create a fresh one.

        Returns:
            TransactionBackend: A new transaction backend instance.

        Raises:
            AssertionError: If the parent :class:`DatabaseBackend` is gone.
        """
        database = self.database
        assert database is not None
        return database.transaction_class(self, existing_transaction)

    @property
    def engine(self) -> AsyncEngine | None:
        """Return the SQLAlchemy :class:`AsyncEngine`, if available.

        Returns:
            AsyncEngine | None: The engine, or ``None``.
        """
        database = self.database
        if database is None:
            return None
        return database.engine


class DatabaseBackend(ABC):
    """Abstract backend managing the database engine and connection pool.

    Attributes:
        engine: The SQLAlchemy :class:`AsyncEngine`, set after
            :meth:`connect` and cleared after :meth:`disconnect`.
        connection_class: The :class:`ConnectionBackend` subclass to use.
        transaction_class: The :class:`TransactionBackend` subclass to use.
        default_batch_size: Default number of rows per batch for
            :meth:`ConnectionBackend.batched_iterate`.
    """

    engine: AsyncEngine | None = None
    connection_class: type[ConnectionBackend]
    transaction_class: type[TransactionBackend]
    default_batch_size: int

    def __init__(
        self,
        connection_class: type[ConnectionBackend],
        transaction_class: type[TransactionBackend],
    ) -> None:
        """Initialise the database backend.

        Args:
            connection_class: Concrete connection backend class.
            transaction_class: Concrete transaction backend class.
        """
        self.connection_class = connection_class
        self.transaction_class = transaction_class

    def __copy__(self) -> DatabaseBackend:
        """Create a shallow copy preserving class references.

        Returns:
            DatabaseBackend: A new instance with the same backend classes.
        """
        return self.__class__(self.connection_class, self.transaction_class)

    @property
    def owner(self) -> RootDatabase | None:
        """Return the high-level :class:`Database` wrapper, if alive.

        Returns:
            RootDatabase | None: The wrapper, or ``None``.
        """
        result = self.__dict__.get("owner")
        if result is None:
            return None
        return cast("RootDatabase", result())

    @owner.setter
    def owner(self, value: RootDatabase) -> None:
        self.__dict__["owner"] = weakref.ref(value)

    @abstractmethod
    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        """Start the database backend and create the engine / pool.

        Args:
            database_url: The sanitised database URL.
            **options: Driver-specific engine options.

        Note:
            ``database_url`` and ``options`` are expected to be sanitised
            by :meth:`extract_options` before this method is called.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Stop the database backend and dispose of the engine / pool."""

    @abstractmethod
    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: dict[str, Any],
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise driver options from the URL query string.

        Options that are encoded in the URL query string are popped from the
        URL and merged into *options*.

        Args:
            database_url: The original database URL.
            **options: Additional caller-supplied options.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: A ``(cleaned_url, options)``
                pair ready for :meth:`connect`.
        """

    def connection(self) -> ConnectionBackend:
        """Create a new :class:`ConnectionBackend` for this database.

        Returns:
            ConnectionBackend: A fresh connection backend instance.
        """
        return self.connection_class(database=self)
