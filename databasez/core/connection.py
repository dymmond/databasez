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
    """Bootstrap a full-isolation database connection on a background loop.

    Connects the database, enters the global connection, and replaces its
    asyncio locks with fresh ones for the new loop.

    Args:
        database: The database instance to connect.
        is_initialized: Threading event signalled once initialisation
            completes.
    """
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
    """Entry point for the full-isolation background thread.

    Runs its own event loop, starts the database, and keeps the loop alive
    until shutdown.  Thread-safe via *_connection_thread_running_lock*.

    Args:
        database: The isolated database copy.
        is_initialized: Signalled when startup completes on the loop.
        _connection_thread_running_lock: Ensures only one thread manages the
            connection at any given time.
    """
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
    """Proxy that dispatches connection methods to a foreign event loop.

    Created automatically by :func:`~databasez.utils.multiloop_protector`
    when a :class:`Connection` method is called from a different event loop
    than the one the connection was created on.

    Supports both plain ``await`` and ``async for`` usage.
    """

    def __init__(
        self,
        connection: Connection,
        fn: Callable,
        args: Any,
        kwargs: Any,
        timeout: float | None,
    ) -> None:
        """Initialise the helper.

        Args:
            connection: The connection to proxy.
            fn: The bound method to call.
            args: Positional arguments.
            kwargs: Keyword arguments.
            timeout: Optional timeout in seconds.
        """
        self.connection = connection
        self.fn = partial(fn, self.connection, *args, **kwargs)
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> Any:
        """Await the proxied method with timeout.

        Returns:
            Any: The proxied result.
        """
        result = await _arun_with_timeout(self.fn(), self.timeout)
        return result

    async def acall(self) -> Any:
        """Schedule :meth:`call` on the connection's event loop.

        Returns:
            Any: The result from the foreign loop.
        """
        return await arun_coroutine_threadsafe(
            self.call(), self.connection._loop, self.connection.poll_interval
        )

    def __await__(self) -> Any:
        return self.acall().__await__()

    async def __aiter__(self) -> Any:
        """Iterate over an async generator proxied to a foreign loop."""
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
    """A high-level database connection with query and transaction support.

    Connections are acquired from a :class:`Database` via
    :meth:`Database.connection` and must be used as async context managers
    to ensure proper resource cleanup.

    Supports *full isolation* mode where the connection runs on a dedicated
    background thread with its own event loop, useful for drivers that do
    not natively support asyncio.

    Attributes:
        async_helper: The helper class used for cross-loop proxying.
    """

    # async helper
    async_helper: type[AsyncHelperConnection] = AsyncHelperConnection

    def __init__(
        self, database: Database, force_rollback: bool = False, full_isolation: bool = False
    ) -> None:
        """Initialise a connection wrapper.

        Args:
            database: The parent :class:`Database`.
            force_rollback: If ``True``, all connection-level transactions
                will be rolled back automatically.
            full_isolation: If ``True``, the connection runs on a dedicated
                background thread with a separate event loop.
        """
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
        """Return the raw backend connection (loop-protected).

        Returns:
            interfaces.ConnectionBackend: The backend connection.
        """
        return self._connection

    @multiloop_protector(False, passthrough_timeout=True)
    async def _aenter(self) -> None:
        """Acquire the backend connection and set up the connection transaction.

        If this is the first acquisition, the underlying backend connection
        is acquired from the pool.  Force-rollback connections also start
        their rollback transaction here.
        """
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
            except BaseException:
                self._connection_counter -= 1
                raise

    async def __aenter__(self) -> Connection:
        """Enter the connection context.

        For full-isolation connections, starts the background thread and
        waits for it to initialise.  For normal connections, delegates
        to :meth:`_aenter`.

        Returns:
            Connection: ``self``.

        Raises:
            Exception: If the isolation thread fails to start.
        """
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
        """Internal: release the connection if the counter drops to zero.

        Returns:
            bool: ``True`` if the connection was fully released.
        """
        closing = False
        async with self._connection_lock:
            assert self._connection is not None
            if self._connection_counter == 0:
                # The global force-rollback connection may be created but never
                # entered; treat close as a no-op in that state.
                if self._connection.async_connection is None:
                    return False
                raise RuntimeError("Connection counter is desynchronized")
            if self._connection_counter < 0:
                raise RuntimeError("Connection context exited too many times")
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

    @multiloop_protector(False, passthrough_timeout=True)
    async def _aexit(self) -> Thread | None:
        """Loop-protected exit. Stops the isolation thread if applicable.

        Returns:
            Thread | None: The isolation thread to join, or ``None``.
        """
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
        """Exit the connection context and join the isolation thread."""
        thread = await self._aexit()
        if thread is not None and thread is not current_thread():
            while thread.is_alive():  # noqa: ASYNC110
                await asyncio.sleep(self.poll_interval)
            thread.join(1)

    @property
    def _loop(self) -> asyncio.AbstractEventLoop | None:
        """The event loop of the parent database."""
        return self._database._loop

    @property
    def poll_interval(self) -> float:
        """The poll interval of the originating database.

        Raises:
            RuntimeError: If polling is disabled (negative interval).
        """
        if self._orig_database.poll_interval < 0:
            raise RuntimeError("Not supposed to run in the poll path")
        return self._orig_database.poll_interval

    @property
    def _backend(self) -> interfaces.DatabaseBackend:
        """The backend of the parent database."""
        return self._database.backend

    @multiloop_protector(False)
    async def fetch_all(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> list[interfaces.Record]:
        """Execute *query* and return all result rows.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            timeout: Optional timeout in seconds.

        Returns:
            list[interfaces.Record]: All result rows.
        """
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_all(built_query)

    @multiloop_protector(False)
    async def fetch_one(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        pos: int = 0,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> interfaces.Record | None:
        """Execute *query* and return a single result row.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            pos: Row position (0-based, ``-1`` for last).
            timeout: Optional timeout in seconds.

        Returns:
            interfaces.Record | None: The requested row, or ``None``.
        """
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query, pos=pos)

    @multiloop_protector(False)
    async def fetch_val(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        column: int | str = 0,
        pos: int = 0,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> Any:
        """Execute *query* and return a single scalar value.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            column: Column index or name.
            pos: Row position (0-based).
            timeout: Optional timeout in seconds.

        Returns:
            Any: The scalar value, or ``None``.
        """
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_val(built_query, column, pos=pos)

    @multiloop_protector(False)
    async def execute(
        self,
        query: ClauseElement | str,
        values: Any = None,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> interfaces.Record | int:
        """Execute a statement and return a concise result.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            timeout: Optional timeout in seconds.

        Returns:
            interfaces.Record | int: Primary-key row / lastrowid / rowcount.
        """
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
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> Sequence[interfaces.Record] | int:
        """Execute a statement with multiple parameter sets.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: A sequence of parameter mappings.
            timeout: Optional timeout in seconds.

        Returns:
            Sequence[interfaces.Record] | int: Primary-key rows / rowcount.
        """
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
        """Execute *query* and yield rows one by one.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            chunk_size: Backend batch-size hint.
            timeout: Per-row timeout in seconds.

        Yields:
            interfaces.Record: Result rows.
        """
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
        """Execute *query* and yield rows in batches.

        Args:
            query: SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.
            batch_size: Number of rows per batch.
            batch_wrapper: Callable to transform each batch (default ``tuple``).
            timeout: Per-batch timeout in seconds.

        Yields:
            BatchCallableResult: A batch of result rows.
        """
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
        timeout: float | None = None,  # stub for multiloop_protector
        **kwargs: Any,
    ) -> Any:
        """Run a synchronous callable on the connection.

        Args:
            fn: A synchronous function.
            *args: Positional arguments for *fn*.
            timeout: Optional timeout in seconds.
            **kwargs: Keyword arguments for *fn*.

        Returns:
            Any: The return value of *fn*.
        """
        async with self._query_lock:
            return await self._connection.run_sync(fn, *args, **kwargs)

    @multiloop_protector(False)
    async def create_all(
        self,
        meta: MetaData,
        timeout: float | None = None,  # stub for multiloop_protector
        **kwargs: Any,
    ) -> None:
        """Create all tables defined in *meta*.

        Args:
            meta: A SQLAlchemy :class:`~sqlalchemy.MetaData` instance.
            timeout: Optional timeout in seconds.
            **kwargs: Extra arguments forwarded to ``meta.create_all``.
        """
        await self.run_sync(meta.create_all, **kwargs)

    @multiloop_protector(False)
    async def drop_all(
        self,
        meta: MetaData,
        timeout: float | None = None,  # stub for multiloop_protector
        **kwargs: Any,
    ) -> None:
        """Drop all tables defined in *meta*.

        Args:
            meta: A SQLAlchemy :class:`~sqlalchemy.MetaData` instance.
            timeout: Optional timeout in seconds.
            **kwargs: Extra arguments forwarded to ``meta.drop_all``.
        """
        await self.run_sync(meta.drop_all, **kwargs)

    def transaction(self, *, force_rollback: bool = False, **kwargs: Any) -> Transaction:
        """Create a new :class:`Transaction` on this connection.

        Args:
            force_rollback: If ``True``, always roll back on exit.
            **kwargs: Extra options forwarded to the transaction backend.

        Returns:
            Transaction: A new transaction instance.
        """
        return Transaction(weakref.ref(self), force_rollback, **kwargs)

    @property
    @multiloop_protector(True)
    def async_connection(self) -> Any:
        """The first layer (SQLAlchemy) async connection handle."""
        return self._connection.async_connection

    @multiloop_protector(False)
    async def get_raw_connection(
        self,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> Any:
        """Return the real raw driver connection.

        Returns:
            Any: The underlying driver connection.
        """
        return await self.async_connection.get_raw_connection()

    @staticmethod
    def _build_query(query: ClauseElement | str, values: Any | None = None) -> ClauseElement:
        """Convert a raw SQL string into a SQLAlchemy text clause with binds.

        If *query* is already a clause element, bind values are attached via
        ``.values()`` when *values* is a dict-like object.

        Args:
            query: A SQL string or SQLAlchemy clause element.
            values: Optional bind parameters.

        Returns:
            ClauseElement: A clause element ready for execution.
        """
        if isinstance(query, str):
            query = text(query)

            return query.bindparams(**values) if values is not None else query
        elif values:
            return query.values(values)  # type: ignore

        return query
