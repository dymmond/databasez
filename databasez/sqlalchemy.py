"""
Forwarding implementations of interfaces to sqlalchemy.

Adjustments should be inherit from here.
"""

from __future__ import annotations

import logging
import typing

from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.sql import ClauseElement

from databasez.interfaces import ConnectionBackend, DatabaseBackend, Record, TransactionBackend

if typing.TYPE_CHECKING:
    from databasez.core import DatabaseURL

logger = logging.getLogger("databasez")


class SQLAlchemyTransaction(TransactionBackend):
    _transaction: typing.Optional[typing.Any] = None

    async def start(self, is_root: bool, extra_options: typing.Dict[str, typing.Any]) -> None:
        connection = self.connection.async_connection
        assert connection is not None, "Connection is not acquired"
        assert self._transaction is None, "Transaction is already initialized"
        self.connection.async_connection = connection = await connection.execution_options(
            **extra_options
        )
        assert (
            await connection.get_isolation_level() != "AUTOCOMMIT"
        ), "transactions doesn't work with AUTOCOMMIT. Please specify another transaction level."
        if not connection.in_transaction():
            self._transaction = await connection.begin()
        else:
            self._transaction = await connection.begin_nested()

    async def commit(self) -> None:
        assert self._transaction is not None, "Transaction is not initialized"
        await self._transaction.commit()
        await self._transaction.close()

    async def rollback(self) -> None:
        assert self._transaction is not None, "Transaction is not initialized"
        await self._transaction.rollback()
        await self._transaction.close()


class SQLAlchemyConnection(ConnectionBackend):
    async_connection: typing.Optional[AsyncConnection] = None

    async def acquire(self) -> None:
        assert self.engine is not None, "Database is not started"
        assert self.async_connection is None, "Connection is already acquired"
        self.async_connection = await self.engine.connect()

    async def release(self) -> None:
        assert self.async_connection is not None, "Connection is not acquired"
        connection, self.async_connection = self.async_connection, None
        await connection.close()

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        with await connection.execute(query) as result:
            return result.fetchall()

    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> typing.Optional[Record]:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        if pos > 0:
            query = query.offset(pos)
        if pos >= 0 and hasattr(query, "limit"):
            query = query.limit(1)
        with await connection.execute(query) as result:
            if pos >= 0:
                return result.first()
            elif pos == -1:
                return await result.last()
            else:
                raise NotImplementedError(
                    f"Only positive numbers and -1 for the last result are currently supported: {pos}"
                )

    async def batched_iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> typing.AsyncGenerator[typing.Any, None]:
        connection = self.async_connection
        if batch_size is None:
            batch_size = 100
        assert connection is not None, "Connection is not acquired"

        async with (await connection.execution_options(yield_per=batch_size)).stream(
            query
        ) as result:
            async for batch in result.partitions():
                yield batch

    async def execute_raw(self, stmt: typing.Any) -> int:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.execute(stmt)

    async def execute_many(self, stmts: typing.List[typing.Any]) -> None:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        for stmt in stmts:
            with await connection.execute(stmt):
                pass

    async def get_raw_connection(self) -> typing.Any:
        """The real raw connection."""
        return await self.async_connection.get_raw_connection()

    def in_transaction(self) -> bool:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return connection.in_transaction()

    async def commit(self) -> None:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        await connection.commit()

    async def rollback(self) -> None:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        await connection.rollback()


class SQLAlchemyDatabase(DatabaseBackend):
    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple[DatabaseURL, typing.Dict[str, typing.Any]]:
        new_query_options = dict(database_url.options)
        if "ssl" in new_query_options:
            assert "ssl" not in options
            ssl = new_query_options.pop("ssl")
            options["ssl"] = {"true": True, "false": False}.get(ssl.lower(), ssl.lower())
        return database_url.replace(options=new_query_options), options

    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        self.engine = create_async_engine(database_url.sqla_url, **options)

    async def disconnect(self) -> None:
        await self.engine.dispose()
