from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable, Iterable, Sequence
from itertools import islice
from typing import TYPE_CHECKING, Any, cast

import orjson
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

try:
    from sqlalchemy.sql import ClauseElement
except Exception:
    ClauseElement = Any

from databasez.interfaces import ConnectionBackend, DatabaseBackend, Record, TransactionBackend

if TYPE_CHECKING:
    from databasez.core import DatabaseURL

logger = logging.getLogger("databasez")


def batched(iterable: Iterable[Any], n: int) -> Any:
    """Yield successive *n*-sized tuples from *iterable*.

    Drop-in replacement for :func:`itertools.batched` (Python 3.12+).

    Args:
        iterable: The source iterable.
        n: Batch size.

    Yields:
        tuple: A batch of up to *n* items.
    """
    iterator = iter(iterable)
    batch = tuple(islice(iterator, n))
    while batch:
        yield batch
        batch = tuple(islice(iterator, n))


class SQLAlchemyTransaction(TransactionBackend):
    """SQLAlchemy-backed transaction with configurable isolation levels.

    Handles starting nested or root transactions, setting and restoring
    isolation levels, and committing / rolling back.

    Attributes:
        raw_transaction: The underlying SQLAlchemy transaction object.
        old_transaction_level: The isolation level prior to this transaction,
            restored on close.
    """

    raw_transaction: Any | None = None
    old_transaction_level: str = ""

    async def start(self, is_root: bool, **extra_options: Any) -> None:
        """Begin the transaction, optionally setting isolation level.

        Args:
            is_root: Whether this is the outermost transaction.
            **extra_options: Extra options; ``isolation_level`` is handled
                specially.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        assert self.raw_transaction is None, "Transaction is already initialized"
        in_transaction = connection.in_transaction()

        self.old_transaction_level = await connection.get_isolation_level()
        if "isolation_level" not in extra_options and not in_transaction:
            isolation_level = self.get_default_transaction_isolation_level(
                is_root, **extra_options
            )
            extra_options["isolation_level"] = isolation_level

        if (
            extra_options.get("isolation_level") is None
            or cast(str, extra_options["isolation_level"]) == self.old_transaction_level
        ):
            extra_options.pop("isolation_level", None)
            self.old_transaction_level = ""
        if extra_options:
            await connection.execution_options(**extra_options)
        assert await connection.get_isolation_level() != "AUTOCOMMIT", (
            "transactions doesn't work with AUTOCOMMIT. Please specify another transaction level."
        )
        if in_transaction:
            self.raw_transaction = await connection.begin_nested()
        else:
            self.raw_transaction = await connection.begin()

    async def _close(self) -> None:
        """Close the transaction and restore the previous isolation level."""
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.close()
        connection = self.async_connection
        if connection is not None and self.old_transaction_level:
            await connection.execution_options(isolation_level=self.old_transaction_level)

    async def commit(self) -> None:
        """Commit the transaction."""
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.commit()
        await self._close()

    async def rollback(self) -> None:
        """Roll back the transaction."""
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.rollback()
        await self._close()

    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        """Return ``SERIALIZABLE`` as the default isolation level.

        Args:
            is_root: Whether this is the outermost transaction.
            **extra_options: Ignored.

        Returns:
            str: ``"SERIALIZABLE"``.
        """
        return "SERIALIZABLE"


class SQLAlchemyConnection(ConnectionBackend):
    """SQLAlchemy-backed connection providing query execution and iteration.

    Attributes:
        async_connection: The SQLAlchemy :class:`AsyncConnection`, set
            after :meth:`acquire` and cleared after :meth:`release`.
    """

    async_connection: AsyncConnection | None = None

    async def acquire(self) -> Any | None:
        """Acquire a connection from the engine.

        Returns:
            Any | None: An existing transaction handle, or ``None``.

        Raises:
            AssertionError: If the engine is not started or the connection
                is already acquired.
        """
        assert self.engine is not None, "Database is not started"
        assert self.async_connection is None, "Connection is already acquired"
        self.async_connection = await self.engine.connect()
        return self.async_connection.get_transaction()

    async def release(self) -> None:
        """Release the connection back to the engine.

        Raises:
            AssertionError: If the connection is not acquired.
        """
        assert self.async_connection is not None, "Connection is not acquired"
        connection, self.async_connection = self.async_connection, None
        await connection.close()

    async def fetch_all(self, query: ClauseElement) -> list[Record]:
        """Execute *query* and return all rows.

        Args:
            query: A compiled clause element.

        Returns:
            list[Record]: All result rows.
        """
        with await self.execute_raw(query) as result:
            return cast(list[Record], result.fetchall())

    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> Record | None:
        """Execute *query* and return a single row.

        Args:
            query: A compiled clause element.
            pos: Row position (0-based; ``-1`` for last).

        Returns:
            Record | None: The requested row.

        Raises:
            NotImplementedError: For positions other than >= 0 or -1.
        """
        if pos > 0:
            query = query.offset(pos)
        if pos >= 0 and hasattr(query, "limit"):
            query = query.limit(1)
        with await self.execute_raw(query) as result:
            if pos >= 0:
                return cast(Record | None, result.first())
            elif pos == -1:
                return cast(Record | None, result.last())
            else:
                raise NotImplementedError(
                    f"Only positive numbers and -1 for the last result are currently supported: {pos}"
                )

    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
        """Execute *query* and yield rows in batches.

        When the dialect supports server-side cursors, streaming is used.
        Otherwise all rows are fetched up front and batched in memory.

        Args:
            query: A compiled clause element.
            batch_size: Rows per batch (default: backend default).

        Yields:
            Sequence[Record]: A batch of rows.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        database = self.database
        assert database
        if batch_size is None:
            batch_size = database.default_batch_size

        if not connection.dialect.supports_server_side_cursors:
            with await self.execute_raw(query) as result:
                for batch in batched(cast(list[Record], result.fetchall()), batch_size):
                    yield batch
                return

        await connection.execution_options(yield_per=batch_size)
        try:
            async with connection.stream(  # type: ignore
                query
            ) as result:
                async for batch in result.partitions():
                    yield batch
        finally:
            # undo the connection change
            await connection.execution_options(yield_per=0)

    async def iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
        """Execute *query* and yield individual rows.

        Uses server-side cursors when supported; otherwise fetches all rows.

        Args:
            query: A compiled clause element.
            batch_size: Backend batch-size hint.

        Yields:
            Record: Individual result rows.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        database = self.database
        assert database
        if batch_size is None:
            batch_size = database.default_batch_size

        if not connection.dialect.supports_server_side_cursors:
            with await self.execute_raw(query) as result:
                for row in result.fetchall():
                    yield row
                return
        await connection.execution_options(yield_per=batch_size)
        try:
            async with connection.stream(  # type: ignore
                query
            ) as result:
                async for row in result:
                    yield row
        finally:
            # undo the connection change
            await connection.execution_options(yield_per=0)

    async def execute_raw(self, stmt: Any, value: Any = None) -> Any:
        """Execute a statement and return the raw result proxy.

        Args:
            stmt: The statement to execute.
            value: Optional bind parameters.

        Returns:
            Any: The SQLAlchemy result proxy.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        if value is not None:
            return await connection.execute(stmt, value)
        return await connection.execute(stmt)

    def parse_execute_result(self, result: Any) -> Record | int:
        """Extract a concise result from a SQLAlchemy result proxy.

        For ``INSERT`` operations, returns the inserted primary key (as a
        :class:`Record`) or ``lastrowid``.  Otherwise returns ``rowcount``.

        Args:
            result: The SQLAlchemy result proxy.

        Returns:
            Record | int: The parsed result.
        """
        if result.is_insert:
            try:
                if result.inserted_primary_key:
                    return cast(Record, result.inserted_primary_key)
            except AttributeError:
                pass
            try:
                return cast(int, result.lastrowid)
            except AttributeError:
                pass
        return cast(int, result.rowcount)

    async def execute(self, stmt: Any, value: Any = None) -> Record | int:
        """Execute a statement and return a concise result.

        For ``INSERT`` statements, returns the inserted primary-key row
        or ``lastrowid``.  For other DML, returns ``rowcount``.

        Args:
            stmt: The statement to execute.
            value: Optional bind parameters.

        Returns:
            Record | int: Primary-key row / lastrowid / rowcount.
        """

        with await self.execute_raw(stmt, value) as result:
            return self.parse_execute_result(result)

    def parse_execute_many_result(self, result: Any) -> Sequence[Record] | int:
        """Extract a concise result from a multi-execute result proxy.

        For ``INSERT`` operations, returns the inserted primary-key rows
        when supported (PostgreSQL).  Otherwise returns ``rowcount``.

        Args:
            result: The SQLAlchemy result proxy.

        Returns:
            Sequence[Record] | int: Primary-key rows / rowcount.
        """
        if result.is_insert:
            try:
                if result.inserted_primary_key_rows is not None:
                    # WARNING: only postgresql, other dbs have None values
                    return cast(Sequence[Record], result.inserted_primary_key_rows)
            except AttributeError:
                pass
        return cast(int, result.rowcount)

    async def execute_many(
        self, stmt: ClauseElement | str, values: Any = None
    ) -> Sequence[Record] | int:
        """Execute a statement with multiple parameter sets.

        Args:
            stmt: The statement to execute.
            values: A sequence of parameter mappings.

        Returns:
            Sequence[Record] | int: Primary-key rows / rowcount.
        """
        with await self.execute_raw(stmt, values) as result:
            return self.parse_execute_many_result(result)

    async def get_raw_connection(self) -> Any:
        """Return the real raw driver connection.

        Returns:
            Any: The underlying driver connection.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.get_raw_connection()

    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run a synchronous callable on the connection.

        Args:
            fn: A synchronous function.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Any: The return value of *fn*.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.run_sync(fn, *args, **kwargs)

    def in_transaction(self) -> bool:
        """Return whether a transaction is currently active.

        Returns:
            bool: ``True`` if a transaction is active.
        """
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return connection.in_transaction()


class SQLAlchemyDatabase(DatabaseBackend):
    """SQLAlchemy-backed database backend.

    Manages the :class:`~sqlalchemy.ext.asyncio.AsyncEngine` lifecycle and
    provides option extraction from the URL query string.

    Attributes:
        default_isolation_level: The isolation level set on engine creation.
            ``"AUTOCOMMIT"`` by default.
        default_batch_size: Default rows per batch for iteration.
    """

    default_isolation_level: str | None = "AUTOCOMMIT"
    default_batch_size: int = 100

    def __copy__(self) -> DatabaseBackend:
        """Create a shallow copy preserving configuration."""
        _copy = super().__copy__()
        _copy.default_isolation_level = self.default_isolation_level
        _copy.default_batch_size = self.default_batch_size
        return _copy

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract engine options from the URL query string.

        Recognises ``ssl``, ``echo``, ``echo_pool``, ``isolation_level``,
        ``pool_size``, ``max_overflow``, and ``pool_recycle``.

        Args:
            database_url: The original URL.
            **options: Additional caller-supplied options.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: Cleaned URL and merged
                options.
        """
        # we have our own logic
        options.setdefault("pool_reset_on_return", None)
        new_query_options = dict(database_url.options)
        for param in ["ssl", "echo", "echo_pool"]:
            if param in new_query_options:
                assert param not in options
                value = cast(str, new_query_options.pop(param))
                options[param] = value.lower() in {"true", ""}
        if "isolation_level" in new_query_options:
            assert "isolation_level" not in options
            options["isolation_level"] = cast(str, new_query_options.pop("isolation_level"))
        for param in ["pool_size", "max_overflow"]:
            if param in new_query_options:
                assert param not in options
                options[param] = int(cast(str, new_query_options.pop(param)))
        if "pool_recycle" in new_query_options:
            assert "pool_recycle" not in options
            options["pool_recycle"] = float(cast(str, new_query_options.pop("pool_recycle")))
        if self.default_isolation_level is not None:
            options.setdefault("isolation_level", self.default_isolation_level)
        return database_url.replace(options=new_query_options), options

    def json_serializer(self, inp: dict) -> str:
        """Serialise a dict to JSON using orjson.

        Args:
            inp: The dictionary to serialise.

        Returns:
            str: A JSON string.
        """
        return orjson.dumps(inp).decode("utf8")

    def json_deserializer(self, inp: str | bytes) -> dict:
        """Deserialise a JSON string or bytes to a dict.

        Args:
            inp: The JSON input.

        Returns:
            dict: The parsed dictionary.
        """
        return cast(dict, orjson.loads(inp))

    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        """Create the async engine from the given URL and options.

        Args:
            database_url: The sanitised database URL.
            **options: Engine creation options.
        """
        self.engine = create_async_engine(database_url.sqla_url, **options)

    async def disconnect(self) -> None:
        """Dispose of the async engine and release all connections."""
        engine = self.engine
        self.engine = None
        assert engine is not None, "database is not initialized"
        await engine.dispose(close=True)
        del engine
