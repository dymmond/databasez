from __future__ import annotations

import asyncio
import typing
import weakref
from contextvars import copy_context
from threading import Event, Thread
from types import TracebackType

from sqlalchemy import text

from databasez import interfaces
from databasez.utils import multiloop_protector

from .transaction import Transaction

if typing.TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.sql import ClauseElement

    from .database import Database


async def _startup(database: Database, is_initialized: Event) -> None:
    await database.connect()
    is_initialized.set()


def _init_thread(database: Database, is_initialized: Event) -> None:
    loop = asyncio.new_event_loop()
    task = loop.create_task(_startup(database, is_initialized))
    try:
        loop.run_forever()
    except RuntimeError:
        pass
    try:
        task.result()
    finally:
        try:
            loop.run_until_complete(database.disconnect())
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            del task
            loop.close()
            database._loop = None


class Connection:
    def __init__(
        self,
        database: Database,
        backend: interfaces.DatabaseBackend,
        force_rollback: bool = False,
        full_isolation: bool = False,
    ) -> None:
        self._full_isolation = full_isolation
        self._database = database
        if full_isolation:
            self._database = database.__class__(
                database, force_rollback=force_rollback, full_isolation=False
            )
            self._database._call_hooks = False
            self._database._global_connection = self
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection.owner = self
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: typing.List[Transaction] = []

        self._query_lock = asyncio.Lock()
        self._force_rollback = force_rollback
        self.connection_transaction: typing.Optional[Transaction] = None
        self._isolation_thread: typing.Optional[Thread] = None

    @multiloop_protector(False)
    async def __aenter__(self) -> Connection:
        if self._full_isolation:
            ctx = copy_context()
            is_initialized = Event()
            self._isolation_thread = thread = Thread(
                target=ctx.run,
                args=[
                    _init_thread,
                    self._database.__class__(
                        self._database, force_rollback=True, full_isolation=False
                    ),
                    is_initialized,
                ],
                daemon=False,
            )
            thread.start()
            is_initialized.wait()
            if not thread.is_alive():
                thread = self._isolation_thread
                self._isolation_thread = None
                thread.join()

        async with self._connection_lock:
            self._connection_counter += 1
            try:
                if self._connection_counter == 1:
                    if self._database._global_connection is self:
                        # on first init double increase, so it isn't terminated too early
                        self._connection_counter += 1
                    raw_transaction = await self._connection.acquire()
                    if raw_transaction is not None:
                        self.connection_transaction = self.transaction(
                            existing_transaction=raw_transaction,
                            force_rollback=self._force_rollback,
                        )
                        # we don't need to call __aenter__ of connection_transaction, it is not on the stack
                    elif self._force_rollback:
                        self.connection_transaction = self.transaction(
                            force_rollback=self._force_rollback
                        )
                        # make re-entrant, we have already the connection lock
                        await self.connection_transaction.start(True)
            except BaseException as e:
                self._connection_counter -= 1
                raise e
        return self

    @multiloop_protector(False)
    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        closing = False
        try:
            async with self._connection_lock:
                assert self._connection is not None
                self._connection_counter -= 1
                if self._connection_counter == 0:
                    closing = True
                    try:
                        if self.connection_transaction:
                            # __aexit__ needs the connection_transaction parameter
                            await self.connection_transaction.__aexit__()
                            # untie, for allowing gc
                            self.connection_transaction = None
                    finally:
                        await self._connection.release()
                        self._database._connection = None
        finally:
            if closing and self._full_isolation:
                assert self._isolation_thread is not None
                thread = self._isolation_thread
                loop = self._database._loop
                self._isolation_thread = None
                if loop:
                    loop.stop()
                thread.join()

    @property
    def _loop(self) -> typing.Any:
        return self._database._loop

    @multiloop_protector(False)
    async def fetch_all(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.List[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_all(built_query)

    @multiloop_protector(False)
    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        pos: int = 0,
    ) -> typing.Optional[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query, pos=pos)

    @multiloop_protector(False)
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

    @multiloop_protector(False)
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

    @multiloop_protector(False)
    async def execute_many(
        self, query: typing.Union[ClauseElement, str], values: typing.Any = None
    ) -> typing.Union[typing.Sequence[interfaces.Record], int]:
        if isinstance(query, str):
            built_query = self._build_query(query, None)
            async with self._query_lock:
                return await self._connection.execute_many(built_query, values)
        else:
            async with self._query_lock:
                return await self._connection.execute_many(query, values)

    @multiloop_protector(False)
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

    @multiloop_protector(False)
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

    @multiloop_protector(False)
    async def run_sync(
        self,
        fn: typing.Callable[..., typing.Any],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        async with self._query_lock:
            return await self._connection.run_sync(fn, *args, **kwargs)

    @multiloop_protector(False)
    async def create_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        await self.run_sync(meta.create_all, **kwargs)

    @multiloop_protector(False)
    async def drop_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        await self.run_sync(meta.drop_all, **kwargs)

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(weakref.ref(self), force_rollback, **kwargs)

    @property
    @multiloop_protector(True)
    def async_connection(self) -> typing.Any:
        """The first layer (sqlalchemy)."""
        return self._connection.async_connection

    @multiloop_protector(False)
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
