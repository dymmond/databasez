"""
Forwarding implementations of interfaces to sqlalchemy.

Adjustments should be inherit from here.
"""

from __future__ import annotations

import logging
import typing

from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import ClauseElement

from databasez.interfaces import ConnectionBackend, DatabaseBackend, Record, TransactionBackend

if typing.TYPE_CHECKING:
    from databasez.core import DatabaseURL

logger = logging.getLogger("databasez")


class SQLAlchemyTransaction(TransactionBackend):
    _transaction: typing.Optional[typing.Any] = None

    async def start(self, is_root: bool, extra_options: typing.Dict[str, typing.Any]) -> None:
        connection = self.connection.raw_connection
        assert connection is not None, "Connection is not acquired"
        assert self._transaction is None, "Transaction is already initialized"
        if extra_options:
            self.connection.raw_connection = connection = await connection.execution_options(
                **extra_options
            )
        if is_root:
            self._transaction = await connection.begin()
        else:
            self._transaction = await connection.begin_nested()

    async def commit(self) -> None:
        assert self._transaction is not None, "Transaction is not initialized"
        await self._transaction.commit()

    async def rollback(self) -> None:
        assert self._transaction is not None, "Transaction is not initialized"
        await self._transaction.rollback()

    async def close(self) -> None:
        assert self._transaction is not None, "Transaction is not initialized"
        await self._transaction.close()


class SQLAlchemyConnection(ConnectionBackend):
    raw_connection: typing.Optional[Connection] = None

    async def acquire(self) -> None:
        assert self.raw_connection is None, "Connection is already acquired"
        self.raw_connection = await self.engine.connect()

    async def release(self) -> None:
        assert self.raw_connection is not None, "Connection is not acquired"
        connection, self.raw_connection = self.raw_connection, None
        await connection.close()

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        connection = self.raw_connection
        assert connection is not None, "Connection is not acquired"
        with await connection.execute(query) as result:
            values = result.fetchall()
            return values

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]:
        connection = self.raw_connection
        assert connection is not None, "Connection is not acquired"
        with await connection.execute(query) as result:
            values = result.fetchone()
            return values

    async def batched_iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> typing.AsyncGenerator[typing.Any, None]:
        connection = self.raw_connection
        if batch_size is None:
            batch_size = 100
        assert connection is not None, "Connection is not acquired"

        async with (await connection.execution_options(yield_per=batch_size)).stream(
            query
        ) as result:
            async for batch in result.partitions():
                yield batch

    async def execute_raw(self, stmt: typing.Any) -> int:
        connection = self.raw_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.execute(stmt)

    async def execute_many(self, stmts: typing.List[typing.Any]) -> None:
        connection = self.raw_connection
        assert connection is not None, "Connection is not acquired"
        for stmt in stmts:
            with await connection.execute(stmt):
                pass

    async def get_raw_connection(self) -> typing.Any:
        return await self.raw_connection.get_raw_connection()


class SQLAlchemyBackend(DatabaseBackend):
    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        self.engine = create_async_engine(str(self.reformat_query(database_url)), **options)

    async def disconnect(self) -> None:
        await self.engine.dispose()
