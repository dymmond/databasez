from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import typing
import weakref
from contextvars import ContextVar
from functools import lru_cache
from types import TracebackType

from databasez import interfaces
from databasez.utils import (
    DATABASEZ_POLL_INTERVAL,
    _arun_with_timeout,
    arun_coroutine_threadsafe,
    multiloop_protector,
)

from .connection import Connection
from .databaseurl import DatabaseURL
from .transaction import Transaction

if typing.TYPE_CHECKING:
    from sqlalchemy import URL, MetaData
    from sqlalchemy.ext.asyncio import AsyncEngine
    from sqlalchemy.sql import ClauseElement

    from databasez.types import BatchCallable, BatchCallableResult, DictAny

try:  # pragma: no cover
    import click

    # Extra log info for optional coloured terminal outputs.
    LOG_EXTRA = {"color_message": "Query: " + click.style("%s", bold=True) + " Args: %s"}
    CONNECT_EXTRA = {"color_message": "Connected to database " + click.style("%s", bold=True)}
    DISCONNECT_EXTRA = {
        "color_message": "Disconnected from database " + click.style("%s", bold=True)
    }
except ImportError:  # pragma: no cover
    LOG_EXTRA = {}
    CONNECT_EXTRA = {}
    DISCONNECT_EXTRA = {}


logger = logging.getLogger("databasez")

default_database: typing.Type[interfaces.DatabaseBackend]
default_connection: typing.Type[interfaces.ConnectionBackend]
default_transaction: typing.Type[interfaces.TransactionBackend]


@lru_cache(1)
def init() -> None:
    """Lazy init global defaults and register sqlalchemy dialects."""
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


ACTIVE_FORCE_ROLLBACKS: ContextVar[
    typing.Optional[weakref.WeakKeyDictionary[ForceRollback, bool]]
] = ContextVar("ACTIVE_FORCE_ROLLBACKS", default=None)


class ForceRollback:
    default: bool

    def __init__(self, default: bool):
        self.default = default

    def set(self, value: typing.Union[bool, None] = None) -> None:
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
        force_rollbacks = ACTIVE_FORCE_ROLLBACKS.get()
        if force_rollbacks is None:
            return self.default
        return force_rollbacks.get(self, self.default)

    @contextlib.contextmanager
    def __call__(self, force_rollback: bool = True) -> typing.Iterator[None]:
        initial = bool(self)
        self.set(force_rollback)
        try:
            yield
        finally:
            self.set(initial)


class ForceRollbackDescriptor:
    def __get__(self, obj: Database, objtype: typing.Type[Database]) -> ForceRollback:
        return obj._force_rollback

    def __set__(self, obj: Database, value: typing.Union[bool, None]) -> None:
        assert value is None or isinstance(value, bool), f"Invalid type: {value!r}."
        obj._force_rollback.set(value)

    def __delete__(self, obj: Database) -> None:
        obj._force_rollback.set(None)


class AsyncHelperDatabase:
    def __init__(
        self,
        database: Database,
        fn: typing.Callable,
        args: typing.Any,
        kwargs: typing.Any,
        timeout: typing.Optional[float],
    ) -> None:
        self.database = database
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> typing.Any:
        async with self.database as database:
            return await _arun_with_timeout(
                self.fn(database, *self.args, **self.kwargs), self.timeout
            )

    def __await__(self) -> typing.Any:
        return self.call().__await__()

    async def __aenter__(self) -> typing.Any:
        database = await self.database.__aenter__()
        self.ctm = await _arun_with_timeout(
            self.fn(database, *self.args, **self.kwargs), timeout=self.timeout
        )
        return await self.ctm.__aenter__()

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
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
    _databases_map: typing.Dict[typing.Any, Database]
    _loop: typing.Optional[asyncio.AbstractEventLoop] = None
    backend: interfaces.DatabaseBackend
    url: DatabaseURL
    options: typing.Any
    is_connected: bool = False
    _call_hooks: bool = True
    _remove_global_connection: bool = True
    _full_isolation: bool = False
    poll_interval: float
    _force_rollback: ForceRollback
    # descriptor
    force_rollback = ForceRollbackDescriptor()
    # async helper
    async_helper: typing.Type[AsyncHelperDatabase] = AsyncHelperDatabase

    def __init__(
        self,
        url: typing.Optional[typing.Union[str, DatabaseURL, URL, Database]] = None,
        *,
        force_rollback: typing.Union[bool, None] = None,
        config: typing.Optional["DictAny"] = None,
        full_isolation: typing.Union[bool, None] = None,
        # for
        poll_interval: typing.Union[float, None] = None,
        **options: typing.Any,
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
                poll_interval = DATABASEZ_POLL_INTERVAL
        self.poll_interval = poll_interval
        self._full_isolation = full_isolation
        self._force_rollback = ForceRollback(force_rollback)
        self.backend.owner = self
        self._connection_map = weakref.WeakKeyDictionary()
        self._databases_map = {}

        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection: typing.Optional[Connection] = None

        self.ref_counter: int = 0
        self.ref_lock: asyncio.Lock = asyncio.Lock()

    def __copy__(self) -> Database:
        return self.__class__(self)

    @property
    def _current_task(self) -> asyncio.Task:
        task = asyncio.current_task()
        if not task:
            raise RuntimeError("No currently active asyncio.Task found")
        return task

    @property
    def _connection(self) -> typing.Optional[Connection]:
        return self._connection_map.get(self._current_task)

    @_connection.setter
    def _connection(self, connection: typing.Optional[Connection]) -> typing.Optional[Connection]:
        task = self._current_task

        if connection is None:
            self._connection_map.pop(task, None)
        else:
            self._connection_map[task] = connection

        return self._connection

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
        """Refcount protected connect hook. Executed begore engine and global connection setup."""

    async def connect(self) -> bool:
        """
        Establish the connection pool.
        """
        loop = asyncio.get_running_loop()
        if self._loop is not None and loop != self._loop:
            if self.poll_interval < 0:
                raise RuntimeError("Subdatabases and polling are disabled")
            # copy when not in map
            if loop not in self._databases_map:
                assert (
                    self._global_connection is not None
                ), "global connection should have been set"
                # correctly initialize force_rollback with parent value
                database = self.__class__(
                    self, force_rollback=bool(self.force_rollback), full_isolation=False
                )
                # prevent side effects of connect_hook
                database._call_hooks = False
                database._global_connection = await self._global_connection.__aenter__()
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
        logger.info("Connected to database %s", self.url.obscure_password, extra=CONNECT_EXTRA)
        self.is_connected = True

        if self._global_connection is None:
            connection = Connection(self, force_rollback=True, full_isolation=self._full_isolation)
            self._global_connection = connection
        else:
            self._remove_global_connection = False
        return True

    async def disconnect_hook(self) -> None:
        """Refcount protected disconnect hook. Executed after connection, engine cleanup."""

    @multiloop_protector(True, inject_parent=True)
    async def disconnect(
        self, force: bool = False, *, parent_database: typing.Optional[Database] = None
    ) -> bool:
        """
        Close all connections in the connection pool.
        """
        # parent_database is injected and should not be specified manually
        if not await self.decr_refcount() or force:
            if not self.is_connected:
                logger.debug("Already disconnected, skipping disconnection")
                return False
            if force:
                logger.warning("Force disconnect, despite refcount not 0")
            else:
                return False
        if parent_database is not None:
            loop = asyncio.get_running_loop()
            del parent_database._databases_map[loop]
        if force:
            if self._databases_map:
                assert not self._databases_map, "sub databases still active, force terminate them"
                for sub_database in self._databases_map.values():
                    await arun_coroutine_threadsafe(
                        sub_database.disconnect(True), sub_database._loop, self.poll_interval
                    )
                self._databases_map = {}
        assert not self._databases_map, "sub databases still active"

        try:
            assert self._global_connection is not None
            await self._global_connection.__aexit__()
            if self._remove_global_connection:
                self._global_connection = None
            self._connection = None
        finally:
            logger.info(
                "Disconnected from database %s",
                self.url.obscure_password,
                extra=DISCONNECT_EXTRA,
            )
            self.is_connected = False
            await self.backend.disconnect()
            self._loop = None
            if self._call_hooks:
                await self.disconnect_hook()
        return True

    async def __aenter__(self) -> "Database":
        await self.connect()
        # get right database
        loop = asyncio.get_running_loop()
        database = self._databases_map.get(loop, self)
        return database

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        await self.disconnect()

    async def fetch_all(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        timeout: typing.Optional[float] = None,
    ) -> typing.List[interfaces.Record]:
        async with self.connection() as connection:
            return await connection.fetch_all(query, values, timeout=timeout)

    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        pos: int = 0,
        timeout: typing.Optional[float] = None,
    ) -> typing.Optional[interfaces.Record]:
        async with self.connection() as connection:
            return await connection.fetch_one(query, values, pos=pos, timeout=timeout)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
        pos: int = 0,
        timeout: typing.Optional[float] = None,
    ) -> typing.Any:
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
        query: typing.Union[ClauseElement, str],
        values: typing.Any = None,
        timeout: typing.Optional[float] = None,
    ) -> typing.Union[interfaces.Record, int]:
        async with self.connection() as connection:
            return await connection.execute(query, values, timeout=timeout)

    async def execute_many(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Any = None,
        timeout: typing.Optional[float] = None,
    ) -> typing.Union[typing.Sequence[interfaces.Record], int]:
        async with self.connection() as connection:
            return await connection.execute_many(query, values, timeout=timeout)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        chunk_size: typing.Optional[int] = None,
        timeout: typing.Optional[float] = None,
    ) -> typing.AsyncGenerator[interfaces.Record, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(query, values, chunk_size, timeout=timeout):
                yield record

    async def batched_iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
        batch_wrapper: BatchCallable = tuple,
        timeout: typing.Optional[float] = None,
    ) -> typing.AsyncGenerator[BatchCallableResult, None]:
        async with self.connection() as connection:
            async for batch in typing.cast(
                typing.AsyncGenerator["BatchCallableResult", None],
                connection.batched_iterate(
                    query,
                    values,
                    batch_wrapper=batch_wrapper,
                    batch_size=batch_size,
                    timeout=timeout,
                ),
            ):
                yield batch

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    async def run_sync(
        self,
        fn: typing.Callable[..., typing.Any],
        *args: typing.Any,
        timeout: typing.Optional[float] = None,
        **kwargs: typing.Any,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.run_sync(fn, *args, **kwargs, timeout=timeout)

    async def create_all(
        self, meta: MetaData, timeout: typing.Optional[float] = None, **kwargs: typing.Any
    ) -> None:
        async with self.connection() as connection:
            await connection.create_all(meta, **kwargs, timeout=timeout)

    async def drop_all(
        self, meta: MetaData, timeout: typing.Optional[float] = None, **kwargs: typing.Any
    ) -> None:
        async with self.connection() as connection:
            await connection.drop_all(meta, **kwargs, timeout=timeout)

    @multiloop_protector(False)
    def _non_global_connection(
        self,
        timeout: typing.Optional[
            float
        ] = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> Connection:
        if self._connection is None:
            _connection = self._connection = Connection(self)
            return _connection
        return self._connection

    def connection(self, timeout: typing.Optional[float] = None) -> Connection:
        if not self.is_connected:
            raise RuntimeError("Database is not connected")
        if self.force_rollback:
            return typing.cast(Connection, self._global_connection)
        return self._non_global_connection(timeout=timeout)

    @property
    @multiloop_protector(True)
    def engine(self) -> typing.Optional[AsyncEngine]:
        return self.backend.engine

    @classmethod
    def get_backends(
        cls,
        # let scheme empty for direct imports
        scheme: str = "",
        *,
        overwrite_paths: typing.Sequence[str] = ["databasez.overwrites"],
        database_name: str = "Database",
        connection_name: str = "Connection",
        transaction_name: str = "Transaction",
        database_class: typing.Optional[typing.Type[interfaces.DatabaseBackend]] = None,
        connection_class: typing.Optional[typing.Type[interfaces.ConnectionBackend]] = None,
        transaction_class: typing.Optional[typing.Type[interfaces.TransactionBackend]] = None,
    ) -> typing.Tuple[
        typing.Type[interfaces.DatabaseBackend],
        typing.Type[interfaces.ConnectionBackend],
        typing.Type[interfaces.TransactionBackend],
    ]:
        module: typing.Any = None
        for overwrite_path in overwrite_paths:
            imp_path = f"{overwrite_path}.{scheme.replace('+', '_')}" if scheme else overwrite_path
            try:
                module = importlib.import_module(imp_path)
            except ImportError as exc:
                logging.debug(
                    f'Import of "{imp_path}" failed. This is not an error.', exc_info=exc
                )
                if "+" in scheme:
                    imp_path = f"{overwrite_path}.{scheme.split('+', 1)[0]}"
                    try:
                        module = importlib.import_module(imp_path)
                    except ImportError as exc:
                        logging.debug(
                            f'Import of "{imp_path}" failed. This is not an error.', exc_info=exc
                        )
            if module is not None:
                break
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
        url: typing.Union[DatabaseURL, str],
        *,
        overwrite_paths: typing.Sequence[str] = ["databasez.overwrites"],
        **options: typing.Any,
    ) -> typing.Tuple[interfaces.DatabaseBackend, DatabaseURL, typing.Dict[str, typing.Any]]:
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
