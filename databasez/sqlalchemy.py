"""
Forwarding implementations of interfaces to sqlalchemy.

Adjustments should be inherit from here.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable, Iterable, Sequence
from itertools import islice
from typing import TYPE_CHECKING, Any, Optional, cast

import orjson
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.sql import ClauseElement

from databasez.interfaces import ConnectionBackend, DatabaseBackend, Record, TransactionBackend

if TYPE_CHECKING:
    from databasez.core import DatabaseURL

logger = logging.getLogger("databasez")


def batched(iterable: Iterable[Any], n: int) -> Any:
    # dropin, batched is not available for pythpn < 3.12
    iterator = iter(iterable)
    batch = tuple(islice(iterator, n))
    while batch:
        yield batch
        batch = tuple(islice(iterator, n))


class SQLAlchemyTransaction(TransactionBackend):
    raw_transaction: Any | None = None
    old_transaction_level: str = ""

    async def start(self, is_root: bool, **extra_options: Any) -> None:
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
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.close()
        connection = self.async_connection
        if connection is not None and self.old_transaction_level:
            await connection.execution_options(isolation_level=self.old_transaction_level)

    async def commit(self) -> None:
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.commit()
        await self._close()

    async def rollback(self) -> None:
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.rollback()
        await self._close()

    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        return "SERIALIZABLE"


class SQLAlchemyConnection(ConnectionBackend):
    async_connection: AsyncConnection | None = None

    async def acquire(self) -> Any | None:
        assert self.engine is not None, "Database is not started"
        assert self.async_connection is None, "Connection is already acquired"
        self.async_connection = await self.engine.connect()
        return self.async_connection.get_transaction()

    async def release(self) -> None:
        assert self.async_connection is not None, "Connection is not acquired"
        connection, self.async_connection = self.async_connection, None
        await connection.close()

    async def fetch_all(self, query: ClauseElement) -> list[Record]:
        with await self.execute_raw(query) as result:
            return cast(list[Record], result.fetchall())

    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> Record | None:
        if pos > 0:
            query = query.offset(pos)
        if pos >= 0 and hasattr(query, "limit"):
            query = query.limit(1)
        with await self.execute_raw(query) as result:
            if pos >= 0:
                return cast(Optional[Record], result.first())
            elif pos == -1:
                return cast(Optional[Record], result.last())
            else:
                raise NotImplementedError(
                    f"Only positive numbers and -1 for the last result are currently supported: {pos}"
                )

    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
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
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        if value is not None:
            return await connection.execute(stmt, value)
        return await connection.execute(stmt)

    def parse_execute_result(self, result: Any) -> Record | int:
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
        """
        Executes statement and returns the last row defaults (insert) or rowid (insert) or the row count of updates.
        """

        with await self.execute_raw(stmt, value) as result:
            return self.parse_execute_result(result)

    def parse_execute_many_result(self, result: Any) -> Sequence[Record] | int:
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
        with await self.execute_raw(stmt, values) as result:
            return self.parse_execute_many_result(result)

    async def get_raw_connection(self) -> Any:
        """The real raw connection."""
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.get_raw_connection()

    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.run_sync(fn, *args, **kwargs)

    def in_transaction(self) -> bool:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return connection.in_transaction()


class SQLAlchemyDatabase(DatabaseBackend):
    default_isolation_level: str | None = "AUTOCOMMIT"
    default_batch_size: int = 100

    def __copy__(self) -> DatabaseBackend:
        _copy = super().__copy__()
        _copy.default_isolation_level = self.default_isolation_level
        _copy.default_batch_size = self.default_batch_size
        return _copy

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
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
            options["isolation_level"] = cast(str, new_query_options.pop(param))
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
        return orjson.dumps(inp).decode("utf8")

    def json_deserializer(self, inp: str | bytes) -> dict:
        return cast(dict, orjson.loads(inp))

    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        self.engine = create_async_engine(database_url.sqla_url, **options)

    async def disconnect(self) -> None:
        engine = self.engine
        self.engine = None
        assert engine is not None, "database is not initialized"
        await engine.dispose(close=True)
        del engine
