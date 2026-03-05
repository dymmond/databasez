from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import weakref
from collections.abc import AsyncGenerator, Callable, Iterator, Sequence
from contextvars import ContextVar
from functools import lru_cache
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast, overload

from monkay.asgi import ASGIApp, LifespanHook

from databasez import interfaces, utils
from databasez.utils import (
    _arun_with_timeout,
    arun_coroutine_threadsafe,
    multiloop_protector,
)

from .connection import Connection
from .databaseurl import DatabaseURL
from .transaction import Transaction

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import AsyncEngine

    try:
        from sqlalchemy.sql import ClauseElement
    except ImportError:
        ClauseElement = Any

    from databasez.types import BatchCallable, BatchCallableResult, DictAny

logger = logging.getLogger("databasez")

default_database: type[interfaces.DatabaseBackend]
default_connection: type[interfaces.ConnectionBackend]
default_transaction: type[interfaces.TransactionBackend]


@lru_cache(1)
def init() -> None:
    """Lazily initialise global defaults and register SQLAlchemy dialects.

    This function is called exactly once (on first :class:`Database`
    construction) to register the ``dbapi2`` and ``jdbc`` dialect entry
    points with SQLAlchemy and to discover the default SQLAlchemy-backed
    backend classes.
    """
    global default_database, default_connection, default_transaction
    from sqlalchemy.dialects import registry

    registry.register("dbapi2", "databasez.dialects.dbapi2", "dialect")
    registry.register("jdbc", "databasez.dialects.jdbc", "dialect")

    default_database, default_connection, default_transaction = Database.get_backends(
        overwrite_paths=["databasez.sqlalchemy"],
        database_name="SQLAlchemyDatabase",
        connection_name="SQLAlchemyConnection",
        transaction_name="SQLAlchemyTransaction",
    )


ACTIVE_FORCE_ROLLBACKS: ContextVar[weakref.WeakKeyDictionary[ForceRollback, bool] | None] = (
    ContextVar("ACTIVE_FORCE_ROLLBACKS", default=None)
)
"""Context-local registry of active force-rollback overrides."""


class ForceRollback:
    """Context-aware boolean flag for controlling force-rollback mode.

    Each :class:`Database` owns a ``ForceRollback`` instance.  Its boolean
    value can be overridden per-context using :meth:`set` or the call
    operator (which works as a context manager).  The default value is used
    when no override is active in the current context.

    Attributes:
        default: The default boolean value when no override is set.
    """

    default: bool

    def __init__(self, default: bool) -> None:
        """Initialise with the given default value.

        Args:
            default: The initial default state.
        """
        self.default = default

    def set(self, value: bool | None = None) -> None:
        """Override the flag in the current context.

        Args:
            value: ``True`` / ``False`` to override, or ``None`` to reset
                to the default for this context.
        """
        force_rollbacks = ACTIVE_FORCE_ROLLBACKS.get()
        if force_rollbacks is None:
            # shortcut, we don't need to initialize anything for None (reset)
            if value is None:
                return
            force_rollbacks = weakref.WeakKeyDictionary()
        else:
            force_rollbacks = force_rollbacks.copy()
        if value is None:
            force_rollbacks.pop(self, None)
        else:
            force_rollbacks[self] = value
        # it is always a copy required to prevent sideeffects between the contexts
        ACTIVE_FORCE_ROLLBACKS.set(force_rollbacks)

    def __bool__(self) -> bool:
        """Return the effective boolean value for the current context."""
        force_rollbacks = ACTIVE_FORCE_ROLLBACKS.get()
        if force_rollbacks is None:
            return self.default
        return force_rollbacks.get(self, self.default)

    @contextlib.contextmanager
    def __call__(self, force_rollback: bool = True) -> Iterator[None]:
        """Context manager to temporarily override the flag.

        Args:
            force_rollback: The value to set while inside the context.

        Yields:
            None
        """
        initial = bool(self)
        self.set(force_rollback)
        try:
            yield
        finally:
            self.set(initial)


class ForceRollbackDescriptor:
    """Descriptor enabling ``database.force_rollback = True/False/None``.

    Setting to ``True`` or ``False`` overrides for the current context.
    Setting to ``None`` (or deleting) resets the override.
    """

    def __get__(self, obj: Database, objtype: type[Database]) -> ForceRollback:
        """Return the :class:`ForceRollback` flag instance."""
        return obj._force_rollback

    def __set__(self, obj: Database, value: bool | None) -> None:
        """Set or reset the force-rollback override."""
        assert value is None or isinstance(value, bool), f"Invalid type: {value!r}."
        obj._force_rollback.set(value)

    def __delete__(self, obj: Database) -> None:
        """Reset the force-rollback override."""
        obj._force_rollback.set(None)


class AsyncHelperDatabase:
    """Proxy that dispatches Database methods to a different event loop.

    Supports both plain ``await`` and async-context-manager usage so that
    database operations transparently work across multiple event loops.
    """

    def __init__(
        self,
        database: Database,
        fn: Callable,
        args: Any,
        kwargs: Any,
        timeout: float | None,
    ) -> None:
        """Initialise the helper.

        Args:
            database: The database to proxy.
            fn: The bound method.
            args: Positional arguments.
            kwargs: Keyword arguments.
            timeout: Optional timeout in seconds.
        """
        self.database = database
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> Any:
        """Connect, execute, and disconnect in one shot.

        Returns:
            Any: The return value of the proxied method.
        """
        async with self.database as database:
            return await _arun_with_timeout(
                self.fn(database, *self.args, **self.kwargs), self.timeout
            )

    def __await__(self) -> Any:
        return self.call().__await__()

    async def __aenter__(self) -> Any:
        """Enter: connect the database and invoke the proxied method."""
        database = await self.database.__aenter__()
        self.ctm = await _arun_with_timeout(
            self.fn(database, *self.args, **self.kwargs), timeout=self.timeout
        )
        return await self.ctm.__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exit: clean up the context manager and disconnect."""
        assert self.ctm is not None
        try:
            await _arun_with_timeout(self.ctm.__aexit__(exc_type, exc_value, traceback), None)
        finally:
            await self.database.__aexit__()


class Database:
    """
    An abstraction on the top of the EncodeORM databases.Database object.

    This object allows to pass also a configuration dictionary in the format of

    DATABASEZ_CONFIG = {
        "connection": {
            "credentials": {
                "scheme": 'sqlite', "postgres"...
                "host": ...,
                "port": ...,
                "user": ...,
                "password": ...,
                "database": ...,
                "options": {
                    "driver": ...
                    "ssl": ...
                }
            }
        }
    }
    """

    _connection_map: weakref.WeakKeyDictionary[asyncio.Task, Connection]
    _databases_map: dict[Any, Database]
    _loop: asyncio.AbstractEventLoop | None = None
    backend: interfaces.DatabaseBackend
    url: DatabaseURL
    options: Any
    is_connected: bool = False
    _call_hooks: bool = True
    _remove_global_connection: bool = True
    _full_isolation: bool = False
    poll_interval: float
    _force_rollback: ForceRollback
    # descriptor
    force_rollback = ForceRollbackDescriptor()
    # async helper
    async_helper: type[AsyncHelperDatabase] = AsyncHelperDatabase

    def __init__(
        self,
        url: str | DatabaseURL | URL | Database | None = None,
        *,
        force_rollback: bool | None = None,
        config: DictAny | None = None,
        full_isolation: bool | None = None,
        # for custom poll intervals
        poll_interval: float | None = None,
        **options: Any,
    ):
        init()
        assert config is None or url is None, "Use either 'url' or 'config', not both."
        if isinstance(url, Database):
            assert not options, "Cannot specify options when copying a Database object."
            self.backend = url.backend.__copy__()
            self.url = url.url
            self.options = url.options
            self._call_hooks = url._call_hooks
            if poll_interval is None:
                poll_interval = url.poll_interval
            if force_rollback is None:
                force_rollback = bool(url.force_rollback)
            if full_isolation is None:
                full_isolation = bool(url._full_isolation)
        else:
            url = DatabaseURL(url)
            if config and "connection" in config:
                connection_config = config["connection"]
                if "credentials" in connection_config:
                    connection_config = connection_config["credentials"]
                    url = url.replace(**connection_config)
            self.backend, self.url, self.options = self.apply_database_url_and_options(
                url, **options
            )
            if force_rollback is None:
                force_rollback = False
            if full_isolation is None:
                full_isolation = False
            if poll_interval is None:
                # when not using utils...., the constant cannot be changed at runtime
                poll_interval = utils.DATABASEZ_POLL_INTERVAL
        self.poll_interval = poll_interval
        self._full_isolation = full_isolation
        self._force_rollback = ForceRollback(force_rollback)
        self.backend.owner = self
        self._connection_map = weakref.WeakKeyDictionary()
        self._databases_map = {}

        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection: Connection | None = None

        self.ref_counter: int = 0
        self.ref_lock: asyncio.Lock = asyncio.Lock()

    def __copy__(self) -> Database:
        """Create a shallow copy of the database (preserving backend state).

        Returns:
            Database: A new Database instance sharing the same backend.
        """
        return self.__class__(self)

    @property
    def _current_task(self) -> asyncio.Task:
        """Return the currently running asyncio task.

        Raises:
            RuntimeError: If no task is active.
        """
        task = asyncio.current_task()
        if not task:
            raise RuntimeError("No currently active asyncio.Task found")
        return task

    @property
    def _connection(self) -> Connection | None:
        """Return the connection bound to the current task, if any."""
        return self._connection_map.get(self._current_task)

    @_connection.setter
    def _connection(self, connection: Connection | None) -> None:
        """Bind or unbind a connection for the current task."""
        task = self._current_task

        if connection is None:
            self._connection_map.pop(task, None)
        else:
            self._connection_map[task] = connection

    async def inc_refcount(self) -> bool:
        """
        Internal method to bump the ref_count.

        Return True if ref_count is 0, False otherwise.

        Should not be used outside of tests. Use connect and hooks instead.
        Not multithreading safe!
        """
        async with self.ref_lock:
            self.ref_counter += 1
            # on the first call is count is 1 because of the former +1
            if self.ref_counter == 1:
                return True
        return False

    async def decr_refcount(self) -> bool:
        """
        Internal method to decrease the ref_count.

        Return True if ref_count drops to 0, False otherwise.

        Should not be used outside of tests. Use disconnect and hooks instead.
        Not multithreading safe!
        """
        async with self.ref_lock:
            self.ref_counter -= 1
            # on the last call, the count is 0
            if self.ref_counter == 0:
                return True
        return False

    async def connect_hook(self) -> None:
        """Hook called before engine setup on first connect.

        Override this in subclasses to perform custom initialisation logic,
        such as creating test databases.  Protected by the ref-counter so
        it runs only on the *first* ``connect()`` call.
        """

    async def connect(self) -> bool:
        """Establish the connection pool.

        If called from a different event loop than the one the database was
        originally connected on, a sub-database is transparently created for
        the current loop.

        Returns:
            bool: ``True`` if this was the first connection (pool created),
                ``False`` if only the ref-counter was incremented.
        """
        loop = asyncio.get_running_loop()
        if self._loop is not None and loop != self._loop:
            if self.poll_interval < 0:
                raise RuntimeError("Subdatabases and polling are disabled")
            # copy when not in map
            if loop not in self._databases_map:
                assert self._global_connection is not None, (
                    "global connection should have been set"
                )
                # correctly initialize force_rollback with parent value
                database = self.__class__(
                    self, force_rollback=bool(self.force_rollback), full_isolation=False
                )
                # prevent side effects of connect_hook
                database._call_hooks = False
                database._global_connection = self._global_connection
                self._databases_map[loop] = database
            # forward call
            return await self._databases_map[loop].connect()

        if not await self.inc_refcount():
            assert self.is_connected, "ref_count < 0"
            return False
        if self._call_hooks:
            try:
                await self.connect_hook()
            except BaseException as exc:
                await self.decr_refcount()
                raise exc
        self._loop = asyncio.get_event_loop()

        await self.backend.connect(self.url, **self.options)
        self.is_connected = True

        if self._global_connection is None:
            connection = Connection(self, force_rollback=True, full_isolation=self._full_isolation)
            self._global_connection = connection
        else:
            self._remove_global_connection = False
        return True

    async def disconnect_hook(self) -> None:
        """Hook called after engine teardown on last disconnect.

        Override this in subclasses to perform custom cleanup logic.
        Protected by the ref-counter so it runs only on the *last*
        ``disconnect()`` call.
        """

    @multiloop_protector(True, inject_parent=True)
    async def disconnect(
        self, force: bool = False, *, parent_database: Database | None = None
    ) -> bool:
        """Close all connections in the connection pool.

        Args:
            force: If ``True``, disconnect even when the ref-counter is
                above zero.
            parent_database: Injected by :func:`multiloop_protector`;
                must not be supplied manually.

        Returns:
            bool: ``True`` if the pool was actually torn down, ``False``
                if only the ref-counter was decremented.
        """
        # parent_database is injected and should not be specified manually
        if not await self.decr_refcount() or force:
            if not self.is_connected:
                logger.debug("Already disconnected, skip disconnecting")
                return False
            if force:
                logger.warning("Force disconnect, despite refcount not 0")
            else:
                return False
        if parent_database is not None:
            loop = asyncio.get_running_loop()
            del parent_database._databases_map[loop]
        if force and self._databases_map:
            for sub_database in self._databases_map.values():
                await arun_coroutine_threadsafe(
                    sub_database.disconnect(True),
                    sub_database._loop,
                    self.poll_interval,
                )
            self._databases_map = {}
        assert not self._databases_map, "sub databases still active"

        try:
            assert self._global_connection is not None
            if self._remove_global_connection:
                await self._global_connection.__aexit__()
                self._global_connection = None
            self._connection = None
        finally:
            self.is_connected = False
            await self.backend.disconnect()
            self._loop = None
            if self._call_hooks:
                await self.disconnect_hook()
        return True

    async def __aenter__(self) -> Database:
        """Connect and return the database for the current loop."""
        await self.connect()
        # get right database
        loop = asyncio.get_running_loop()
        database = self._databases_map.get(loop, self)
        return database

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Disconnect the database."""
        await self.disconnect()

    async def fetch_all(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        timeout: float | None = None,
    ) -> list[interfaces.Record]:
        """Execute *query* and return all result rows.

        Args:
            query: SQL string or clause element.
            values: Optional bind parameters.
            timeout: Optional timeout in seconds.

        Returns:
            list[interfaces.Record]: All result rows.
        """
        async with self.connection() as connection:
            return await connection.fetch_all(query, values, timeout=timeout)

    async def fetch_one(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        pos: int = 0,
        timeout: float | None = None,
    ) -> interfaces.Record | None:
        """Execute *query* and return a single row.

        Args:
            query: SQL string or clause element.
            values: Optional bind parameters.
            pos: Row position (0-based, ``-1`` for last).
            timeout: Optional timeout in seconds.

        Returns:
            interfaces.Record | None: The row, or ``None``.
        """
        async with self.connection() as connection:
            return await connection.fetch_one(query, values, pos=pos, timeout=timeout)

    async def fetch_val(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        column: int | str = 0,
        pos: int = 0,
        timeout: float | None = None,
    ) -> Any:
        """Execute *query* and return a single scalar value.

        Args:
            query: SQL string or clause element.
            values: Optional bind parameters.
            column: Column index or name.
            pos: Row position.
            timeout: Optional timeout in seconds.

        Returns:
            Any: The scalar value, or ``None``.
        """
        async with self.connection() as connection:
            return await connection.fetch_val(
                query,
                values,
                column=column,
                pos=pos,
                timeout=timeout,
            )

    async def execute(
        self,
        query: ClauseElement | str,
        values: Any = None,
        timeout: float | None = None,
    ) -> interfaces.Record | int:
        """Execute a statement and return a concise result.

        Args:
            query: SQL string or clause element.
            values: Optional bind parameters.
            timeout: Optional timeout in seconds.

        Returns:
            interfaces.Record | int: Primary-key row / rowcount.
        """
        async with self.connection() as connection:
            return await connection.execute(query, values, timeout=timeout)

    async def execute_many(
        self,
        query: ClauseElement | str,
        values: Any = None,
        timeout: float | None = None,
    ) -> Sequence[interfaces.Record] | int:
        """Execute a statement with multiple parameter sets.

        Args:
            query: SQL string or clause element.
            values: A sequence of parameter mappings.
            timeout: Optional timeout in seconds.

        Returns:
            Sequence[interfaces.Record] | int: Primary-key rows / rowcount.
        """
        async with self.connection() as connection:
            return await connection.execute_many(query, values, timeout=timeout)

    async def iterate(
        self,
        query: ClauseElement | str,
        values: dict | None = None,
        chunk_size: int | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[interfaces.Record, None]:
        """Execute *query* and yield rows one by one.

        Args:
            query: SQL string or clause element.
            values: Optional bind parameters.
            chunk_size: Backend batch-size hint.
            timeout: Per-row timeout in seconds.

        Yields:
            interfaces.Record: Result rows.
        """
        async with self.connection() as connection:
            async for record in connection.iterate(query, values, chunk_size, timeout=timeout):
                yield record

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
            query: SQL string or clause element.
            values: Optional bind parameters.
            batch_size: Rows per batch.
            batch_wrapper: Callable to transform each batch.
            timeout: Per-batch timeout in seconds.

        Yields:
            BatchCallableResult: Batches of result rows.
        """
        async with self.connection() as connection:
            async for batch in cast(
                AsyncGenerator["BatchCallableResult", None],
                connection.batched_iterate(
                    query,
                    values,
                    batch_wrapper=batch_wrapper,
                    batch_size=batch_size,
                    timeout=timeout,
                ),
            ):
                yield batch

    def transaction(self, *, force_rollback: bool = False, **kwargs: Any) -> Transaction:
        """Create a new :class:`Transaction` on the current connection.

        Args:
            force_rollback: If ``True``, always roll back on exit.
            **kwargs: Extra options forwarded to the transaction backend.

        Returns:
            Transaction: A new transaction instance.
        """
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run a synchronous callable on the current connection.

        Args:
            fn: A synchronous function.
            *args: Positional arguments.
            timeout: Optional timeout in seconds.
            **kwargs: Keyword arguments.

        Returns:
            Any: The return value of *fn*.
        """
        async with self.connection() as connection:
            return await connection.run_sync(fn, *args, **kwargs, timeout=timeout)

    async def create_all(
        self, meta: MetaData, timeout: float | None = None, **kwargs: Any
    ) -> None:
        """Create all tables defined in *meta*.

        Args:
            meta: A SQLAlchemy :class:`~sqlalchemy.MetaData`.
            timeout: Optional timeout in seconds.
            **kwargs: Extra arguments for ``meta.create_all``.
        """
        async with self.connection() as connection:
            await connection.create_all(meta, **kwargs, timeout=timeout)

    async def drop_all(self, meta: MetaData, timeout: float | None = None, **kwargs: Any) -> None:
        """Drop all tables defined in *meta*.

        Args:
            meta: A SQLAlchemy :class:`~sqlalchemy.MetaData`.
            timeout: Optional timeout in seconds.
            **kwargs: Extra arguments for ``meta.drop_all``.
        """
        async with self.connection() as connection:
            await connection.drop_all(meta, **kwargs, timeout=timeout)

    @multiloop_protector(False)
    def _non_global_connection(
        self,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> Connection:
        """Return or create the per-task connection (non-global).

        Returns:
            Connection: The connection for the current task.
        """
        if self._connection is None:
            _connection = self._connection = Connection(self)
            return _connection
        return self._connection

    def connection(self, timeout: float | None = None) -> Connection:
        """Return a connection suitable for the current context.

        In force-rollback mode the global connection is returned; otherwise
        a per-task connection is returned (created on demand).

        Args:
            timeout: Optional timeout for cross-loop proxying.

        Returns:
            Connection: The active connection.

        Raises:
            RuntimeError: If the database is not connected.
        """
        if not self.is_connected:
            raise RuntimeError("Database is not connected")
        if self.force_rollback:
            return cast(Connection, self._global_connection)
        return self._non_global_connection(timeout=timeout)

    @property
    @multiloop_protector(True)
    def engine(self) -> AsyncEngine | None:
        """The SQLAlchemy :class:`AsyncEngine`, if connected."""
        return self.backend.engine

    @overload
    def asgi(
        self,
        app: None,
        handle_lifespan: bool = False,
    ) -> Callable[[ASGIApp], ASGIApp]: ...

    @overload
    def asgi(
        self,
        app: ASGIApp,
        handle_lifespan: bool = False,
    ) -> ASGIApp: ...

    def asgi(
        self,
        app: ASGIApp | None = None,
        handle_lifespan: bool = False,
    ) -> ASGIApp | Callable[[ASGIApp], ASGIApp]:
        """Return wrapper for asgi integration."""

        async def setup() -> contextlib.AsyncExitStack:
            cleanupstack = contextlib.AsyncExitStack()
            await self.connect()
            cleanupstack.push_async_callback(self.disconnect)
            return cleanupstack

        return LifespanHook(app, setup=setup, do_forward=not handle_lifespan)

    @classmethod
    def get_backends(
        cls,
        # let scheme empty for direct imports
        scheme: str = "",
        *,
        overwrite_paths: Sequence[str] = ("databasez.overwrites",),
        database_name: str = "Database",
        connection_name: str = "Connection",
        transaction_name: str = "Transaction",
        database_class: type[interfaces.DatabaseBackend] | None = None,
        connection_class: type[interfaces.ConnectionBackend] | None = None,
        transaction_class: type[interfaces.TransactionBackend] | None = None,
    ) -> tuple[
        type[interfaces.DatabaseBackend],
        type[interfaces.ConnectionBackend],
        type[interfaces.TransactionBackend],
    ]:
        """Discover backend classes for the given scheme.

        Searches *overwrite_paths* for modules that export ``Database``,
        ``Connection``, and ``Transaction`` classes matching the scheme.
        Falls back to the supplied defaults.

        Args:
            scheme: The dialect scheme (e.g. ``"postgresql+asyncpg"``).
            overwrite_paths: Module paths searched for overwrite modules.
            database_name: Attribute name for the database backend class.
            connection_name: Attribute name for the connection backend class.
            transaction_name: Attribute name for the transaction backend class.
            database_class: Fallback database backend class.
            connection_class: Fallback connection backend class.
            transaction_class: Fallback transaction backend class.

        Returns:
            tuple: ``(database_class, connection_class, transaction_class)``.

        Raises:
            AssertionError: If a discovered class does not subclass its
                expected abstract base.
        """
        module: Any = None
        # when not using utils...., the constant cannot be changed at runtime for debug purposes
        more_debug = utils.DATABASEZ_OVERWRITE_LOGGING
        for overwrite_path in overwrite_paths:
            imp_path = f"{overwrite_path}.{scheme.replace('+', '_')}" if scheme else overwrite_path
            try:
                module = importlib.import_module(imp_path)
            except ImportError as exc:
                logging.debug(
                    f'Could not import "{imp_path}". Continue search.',
                    exc_info=exc if more_debug else None,
                )
                if "+" in scheme:
                    imp_path = f"{overwrite_path}.{scheme.split('+', 1)[0]}"
                    try:
                        module = importlib.import_module(imp_path)
                    except ImportError as exc:
                        logging.debug(
                            f'Could not import "{imp_path}". Continue search.',
                            exc_info=exc if more_debug else None,
                        )
            if module is not None:
                break
        if module is None:
            logging.debug(
                "No overwrites found. Use default.",
            )
        database_class = getattr(module, database_name, database_class)
        assert database_class is not None and issubclass(
            database_class, interfaces.DatabaseBackend
        )
        connection_class = getattr(module, connection_name, connection_class)
        assert connection_class is not None and issubclass(
            connection_class, interfaces.ConnectionBackend
        )
        transaction_class = getattr(module, transaction_name, transaction_class)
        assert transaction_class is not None and issubclass(
            transaction_class, interfaces.TransactionBackend
        )
        return database_class, connection_class, transaction_class

    @classmethod
    def apply_database_url_and_options(
        cls,
        url: DatabaseURL | str,
        *,
        overwrite_paths: Sequence[str] = ("databasez.overwrites",),
        **options: Any,
    ) -> tuple[interfaces.DatabaseBackend, DatabaseURL, dict[str, Any]]:
        """Build a backend instance and normalise URL + options.

        Discovers the correct backend classes for the URL's scheme,
        instantiates the database backend, and calls
        :meth:`~DatabaseBackend.extract_options` to normalise the URL and
        merge query-string options.

        Args:
            url: The original database URL.
            overwrite_paths: Module paths searched for overwrite modules.
            **options: Additional caller-supplied engine options.

        Returns:
            tuple: ``(backend, cleaned_url, options)``.

        Raises:
            AssertionError: If the resolved dialect is not async.
        """
        url = DatabaseURL(url)
        database_class, connection_class, transaction_class = cls.get_backends(
            url.scheme,
            database_class=default_database,
            connection_class=default_connection,
            transaction_class=default_transaction,
            overwrite_paths=overwrite_paths,
        )

        backend = database_class(
            connection_class=connection_class, transaction_class=transaction_class
        )
        url, options = backend.extract_options(url, **options)
        # check against transformed url
        assert url.sqla_url.get_dialect(True).is_async, f'Dialect: "{url.scheme}" is not async.'

        return backend, url, options
