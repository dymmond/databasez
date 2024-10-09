from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncGenerator, Callable, Sequence
from functools import partial
from threading import Event, Lock, Thread, current_thread
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import text

from databasez import interfaces
from databasez.utils import _arun_with_timeout, arun_coroutine_threadsafe, multiloop_protector

from .transaction import Transaction

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.sql import ClauseElement

    from databasez.types import BatchCallable, BatchCallableResult

    from .database import Database


async def _startup(database: Database, is_initialized: Event) -> None:
    await database.connect()
    _global_connection = cast(Connection, database._global_connection)
    await _global_connection._aenter()
    # we ensure fresh locks
    _global_connection._query_lock = asyncio.Lock()
    _global_connection._connection_lock = asyncio.Lock()
    _global_connection._transaction_lock = asyncio.Lock()
    is_initialized.set()


def _init_thread(
    database: Database, is_initialized: Event, _connection_thread_running_lock: Lock
) -> None:
    # ensure only thread manages the connection thread at the same time
    # this is only relevant when starting up after a shutdown
    with _connection_thread_running_lock:
        loop = asyncio.new_event_loop()
        # keep reference
        task = loop.create_task(_startup(database, is_initialized))
        try:
            try:
                loop.run_forever()
            except RuntimeError:
                pass
            finally:
                # now all inits wait
                is_initialized.clear()
            loop.run_until_complete(database.disconnect())
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            del task
            loop.close()
            database._loop = None
            database._global_connection._isolation_thread = None  # type: ignore


class AsyncHelperConnection:
    def __init__(
        self,
        connection: Connection,
        fn: Callable,
        args: Any,
        kwargs: Any,
        timeout: float | None,
    ) -> None:
        self.connection = connection
        self.fn = partial(fn, self.connection, *args, **kwargs)
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> Any:
        # is automatically awaited
        result = await _arun_with_timeout(self.fn(), self.timeout)
        return result

    async def acall(self) -> Any:
        return await arun_coroutine_threadsafe(
            self.call(), self.connection._loop, self.connection.poll_interval
        )

    def __await__(self) -> Any:
        return self.acall().__await__()

    async def __aiter__(self) -> Any:
        result = await self.acall()
        try:
            while True:
                yield await arun_coroutine_threadsafe(
                    _arun_with_timeout(result.__anext__(), self.timeout),
                    self.connection._loop,
                    self.connection.poll_interval,
                )
        except StopAsyncIteration:
            pass


class Connection:
    # async helper
    async_helper: type[AsyncHelperConnection] = AsyncHelperConnection

    def __init__(
        self, database: Database, force_rollback: bool = False, full_isolation: bool = False
    ) -> None:
        self._orig_database = self._database = database
        self._full_isolation = full_isolation
        self._connection_thread_lock: Lock | None = None
        self._connection_thread_is_initialized: Event | None = None
        self._connection_thread_running_lock: Lock | None = None
        self._isolation_thread: Thread | None = None
        if self._full_isolation:
            self._connection_thread_lock = Lock()
            self._connection_thread_is_initialized = Event()
            self._connection_thread_running_lock = Lock()
            self._database = database.__class__(
                database, force_rollback=force_rollback, full_isolation=False, poll_interval=-1
            )
            self._database._call_hooks = False
            self._database._global_connection = self
        # the asyncio locks are overwritten in python versions < 3.10 when using full_isolation
        self._query_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._transaction_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection.owner = self
        self._connection_counter = 0

        # for keeping weak references to transactions active
        self._transaction_stack: list[tuple[Transaction, interfaces.TransactionBackend]] = []

        self._force_rollback = force_rollback
        self.connection_transaction: Transaction | None = None

    @multiloop_protector(True)
    def _get_connection_backend(self) -> interfaces.ConnectionBackend:
        return self._connection

    @multiloop_protector(False, passthrough_timeout=True)  # fail when specifying timeout
    async def _aenter(self) -> None:
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
                        await self.connection_transaction.start()
            except BaseException as e:
                self._connection_counter -= 1
                raise e

    async def __aenter__(self) -> Connection:
        initialized: bool = False
        if self._full_isolation:
            thread: Thread | None = None
            assert self._connection_thread_lock is not None
            assert self._connection_thread_is_initialized is not None
            assert self._connection_thread_running_lock is not None
            with self._connection_thread_lock:
                thread = self._isolation_thread
                if thread is None:
                    initialized = True
                    self._isolation_thread = thread = Thread(
                        target=_init_thread,
                        args=[
                            self._database,
                            self._connection_thread_is_initialized,
                            self._connection_thread_running_lock,
                        ],
                        daemon=True,
                    )
                    # must be started with lock held, for setting is_alive
                    thread.start()
            assert thread is not None
            # bypass for full_isolated
            if thread is not current_thread():
                if initialized:
                    while not self._connection_thread_is_initialized.is_set():
                        if not thread.is_alive():
                            with self._connection_thread_lock:
                                self._isolation_thread = None
                                self._connection_thread_is_initialized.clear()
                            thread.join(1)
                            raise Exception("Cannot start full isolation thread")
                        await asyncio.sleep(self.poll_interval)

                else:
                    # ensure to be not in the isolation thread itself
                    while not self._connection_thread_is_initialized.is_set():
                        if not thread.is_alive():
                            raise Exception("Isolation thread is dead")
                        await asyncio.sleep(self.poll_interval)

        if not initialized:
            await self._aenter()
        return self

    async def _aexit_raw(self) -> bool:
        closing = False
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
        return closing

    @multiloop_protector(False, passthrough_timeout=True)  # fail when specifying timeout
    async def _aexit(self) -> Thread | None:
        if self._full_isolation:
            assert self._connection_thread_lock is not None
            # the lock must be held on exit
            with self._connection_thread_lock:
                if await self._aexit_raw():
                    loop = self._database._loop
                    thread = self._isolation_thread
                    if loop is not None and loop.is_running():
                        loop.stop()
                    else:
                        self._isolation_thread = None
                    return thread
        else:
            await self._aexit_raw()
        return None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        thread = await self._aexit()
        if thread is not None and thread is not current_thread():
            while thread.is_alive():  # noqa: ASYNC110
                await asyncio.sleep(self.poll_interval)
            thread.join(1)

    @property
    def _loop(self) -> asyncio.AbstractEventLoop | None:
        return self._database._loop

    @property
    def poll_interval(self) -> float:
        if self._orig_database.poll_interval < 0:
            raise RuntimeError("Not supposed to run in the poll path")
        return self._orig_database.poll_interval

    @property
    def _backend(self) -> interfaces.DatabaseBackend:
        return self._database.backend

    @multiloop_protector(False)
    async def fetch_all(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> list[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_all(built_query)

    @multiloop_protector(False)
    async def fetch_one(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        pos: int = 0,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> interfaces.Record | None:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query, pos=pos)

    @multiloop_protector(False)
    async def fetch_val(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        column: Any = 0,
        pos: int = 0,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> Any:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_val(built_query, column, pos=pos)

    @multiloop_protector(False)
    async def execute(
        self,
        query: ClauseElement | str,
        values: Any = None,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> interfaces.Record | int:
        if isinstance(query, str):
            built_query = self._build_query(query, values)
            async with self._query_lock:
                return await self._connection.execute(built_query)
        else:
            async with self._query_lock:
                return await self._connection.execute(query, values)

    @multiloop_protector(False)
    async def execute_many(
        self,
        query: ClauseElement | str,
        values: Any = None,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> Sequence[interfaces.Record] | int:
        if isinstance(query, str):
            built_query = self._build_query(query, None)
            async with self._query_lock:
                return await self._connection.execute_many(built_query, values)
        else:
            async with self._query_lock:
                return await self._connection.execute_many(query, values)

    @multiloop_protector(False, passthrough_timeout=True)
    async def iterate(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        chunk_size: int | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[interfaces.Record, None]:
        built_query = self._build_query(query, values)
        if timeout is None or timeout <= 0:
            # anext is available in python 3.10

            async def next_fn(inp: Any) -> interfaces.Record:
                return await aiterator.__anext__()
        else:

            async def next_fn(inp: Any) -> interfaces.Record:
                return await asyncio.wait_for(aiterator.__anext__(), timeout=timeout)

        async with self._query_lock:
            aiterator = self._connection.iterate(built_query, chunk_size).__aiter__()
            try:
                while True:
                    yield await next_fn(aiterator)
            except StopAsyncIteration:
                pass

    @multiloop_protector(False, passthrough_timeout=True)
    async def batched_iterate(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        batch_size: int | None = None,
        batch_wrapper: BatchCallable = tuple,
        timeout: float | None = None,
    ) -> AsyncGenerator[BatchCallableResult, None]:
        built_query = self._build_query(query, values)
        if timeout is None or timeout <= 0:
            # anext is available in python 3.10

            async def next_fn(inp: Any) -> Sequence[interfaces.Record]:
                return await aiterator.__anext__()
        else:

            async def next_fn(inp: Any) -> Sequence[interfaces.Record]:
                return await asyncio.wait_for(aiterator.__anext__(), timeout=timeout)

        async with self._query_lock:
            aiterator = self._connection.batched_iterate(built_query, batch_size).__aiter__()
            try:
                while True:
                    yield batch_wrapper(await next_fn(aiterator))
            except StopAsyncIteration:
                pass

    @multiloop_protector(False)
    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
        **kwargs: Any,
    ) -> Any:
        async with self._query_lock:
            return await self._connection.run_sync(fn, *args, **kwargs)

    @multiloop_protector(False)
    async def create_all(
        self,
        meta: MetaData,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
        **kwargs: Any,
    ) -> None:
        await self.run_sync(meta.create_all, **kwargs)

    @multiloop_protector(False)
    async def drop_all(
        self,
        meta: MetaData,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
        **kwargs: Any,
    ) -> None:
        await self.run_sync(meta.drop_all, **kwargs)

    def transaction(self, *, force_rollback: bool = False, **kwargs: Any) -> Transaction:
        return Transaction(weakref.ref(self), force_rollback, **kwargs)

    @property
    @multiloop_protector(True)
    def async_connection(self) -> Any:
        """The first layer (sqlalchemy)."""
        return self._connection.async_connection

    @multiloop_protector(False)
    async def get_raw_connection(
        self,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> Any:
        """The real raw connection (driver)."""
        return await self.async_connection.get_raw_connection()

    @staticmethod
    def _build_query(query: ClauseElement | str, values: Any | None = None) -> ClauseElement:
        if isinstance(query, str):
            query = text(query)

            return query.bindparams(**values) if values is not None else query
        elif values:
            return query.values(values)  # type: ignore

        return query
