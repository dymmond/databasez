from __future__ import annotations

import asyncio
import typing
import weakref
from types import TracebackType

from sqlalchemy import text

from databasez import interfaces

from .transaction import Transaction

if typing.TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.sql import ClauseElement

    from .database import Database


class Connection:
    def __init__(self, database: Database, backend: interfaces.DatabaseBackend) -> None:
        self._database = database
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection.owner = self
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: typing.List[Transaction] = []

        self._query_lock = asyncio.Lock()
        self.connection_transaction: typing.Optional[Transaction] = None

    async def __aenter__(self) -> Connection:
        async with self._connection_lock:
            self._connection_counter += 1
            try:
                if self._connection_counter == 1:
                    raw_transaction = await self._connection.acquire()
                    if raw_transaction is not None:
                        self.connection_transaction = self.transaction(
                            existing_transaction=raw_transaction
                        )
                        # we don't need to call __aenter__ of connection_transaction
            except BaseException as e:
                self._connection_counter -= 1
                raise e
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        async with self._connection_lock:
            assert self._connection is not None
            self._connection_counter -= 1
            if self._connection_counter == 0:
                try:
                    if self.connection_transaction:
                        await self.connection_transaction.__aexit__(exc_type, exc_value, traceback)
                finally:
                    await self._connection.release()
                    self._database._connection = None

    async def fetch_all(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.List[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_all(built_query)

    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        pos: int = 0,
    ) -> typing.Optional[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query, pos=pos)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
        pos: int = 0,
    ) -> typing.Any:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_val(built_query, column, pos=pos)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Any = None,
    ) -> typing.Union[interfaces.Record, int]:
        if isinstance(query, str):
            built_query = self._build_query(query, values)
            async with self._query_lock:
                return await self._connection.execute(built_query)
        else:
            async with self._query_lock:
                return await self._connection.execute(query, values)

    async def execute_many(self, query: typing.Union[ClauseElement, str], values: list) -> None:
        queries = [self._build_query(query, values_set) for values_set in values]
        async with self._query_lock:
            await self._connection.execute_many(queries)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            async for record in self._connection.iterate(built_query, batch_size):
                yield record

    async def batched_iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            async for records in self._connection.batched_iterate(built_query, batch_size):
                yield records

    async def run_sync(
        self,
        fn: typing.Callable[..., typing.Any],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        async with self._query_lock:
            return await self._connection.run_sync(fn, *args, **kwargs)

    async def create_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        await self.run_sync(meta.create_all, **kwargs)

    async def drop_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        await self.run_sync(meta.drop_all, **kwargs)

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(weakref.ref(self), force_rollback, **kwargs)

    @property
    def async_connection(self) -> typing.Any:
        """The first layer (sqlalchemy)."""
        return self._connection.async_connection

    async def get_raw_connection(self) -> typing.Any:
        """The real raw connection (driver)."""
        return await self.async_connection.get_raw_connection()

    @staticmethod
    def _build_query(
        query: typing.Union[ClauseElement, str], values: typing.Optional[typing.Any] = None
    ) -> ClauseElement:
        if isinstance(query, str):
            query = text(query)

            return query.bindparams(**values) if values is not None else query
        elif values:
            return query.values(values)  # type: ignore

        return query
