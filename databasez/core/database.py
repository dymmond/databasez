from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import typing
import weakref
from types import TracebackType

from databasez import interfaces
from databasez.sqlalchemy import SQLAlchemyBackend, SQLAlchemyConnection, SQLAlchemyTransaction

from .connection import Connection
from .databaseurl import DatabaseURL
from .transaction import Transaction

if typing.TYPE_CHECKING:
    from sqlalchemy import MetaData
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
    backend: interfaces.DatabaseBackend

    def __init__(
        self,
        url: typing.Optional[typing.Union[str, DatabaseURL]] = None,
        *,
        force_rollback: bool = False,
        config: typing.Optional["DictAny"] = None,
        **options: typing.Any,
    ):
        assert config is None or url is None, "Use either 'url' or 'config', not both."

        url = DatabaseURL(url)
        if config and "connection" in config:
            connection_config = config["connection"]
            if "credentials" in connection_config:
                connection_config = connection_config["credentials"]
                url = url.replace(**connection_config)
        self.url = url
        self.options = options
        self.is_connected = False
        self._connection_map = weakref.WeakKeyDictionary()

        self._force_rollback = force_rollback

        self.backend = self.get_backend(self.url.scheme)

        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection: typing.Optional[Connection] = None
        self._global_transaction: typing.Optional[Transaction] = None

    @property
    def _current_task(self) -> asyncio.Task:
        task = asyncio.current_task()
        if not task:
            raise RuntimeError("No currently active asyncio.Task found")
        return task

    @property
    def _connection(self) -> typing.Optional["Connection"]:
        return self._connection_map.get(self._current_task)

    @_connection.setter
    def _connection(
        self, connection: typing.Optional["Connection"]
    ) -> typing.Optional["Connection"]:
        task = self._current_task

        if connection is None:
            self._connection_map.pop(task, None)
        else:
            self._connection_map[task] = connection

        return self._connection

    async def connect(self) -> None:
        """
        Establish the connection pool.
        """
        if self.is_connected:
            logger.debug("Already connected, skipping connection")
            return None

        await self.backend.connect(self.url, **self.options)
        logger.info("Connected to database %s", self.url.obscure_password, extra=CONNECT_EXTRA)
        self.is_connected = True

        if self._force_rollback:
            assert self._global_connection is None
            assert self._global_transaction is None

            self._global_connection = Connection(self, self.backend)
            self._global_transaction = self._global_connection.transaction(force_rollback=True)

            await self._global_transaction.__aenter__()

    async def disconnect(self) -> None:
        """
        Close all connections in the connection pool.
        """
        if not self.is_connected:
            logger.debug("Already disconnected, skipping disconnection")
            return None

        if self._force_rollback:
            assert self._global_connection is not None
            assert self._global_transaction is not None

            await self._global_transaction.__aexit__()

            self._global_transaction = None
            self._global_connection = None
        else:
            self._connection = None

        await self.backend.disconnect()
        logger.info(
            "Disconnected from database %s",
            self.url.obscure_password,
            extra=DISCONNECT_EXTRA,
        )
        self.is_connected = False

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

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
    ) -> typing.List[interfaces.Record]:
        async with self.connection() as connection:
            return await connection.fetch_all(query, values)

    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Optional[interfaces.Record]:
        async with self.connection() as connection:
            return await connection.fetch_one(query, values)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.fetch_val(query, values, column=column)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.execute(query, values)

    async def execute_many(self, query: typing.Union[ClauseElement, str], values: list) -> None:
        async with self.connection() as connection:
            return await connection.execute_many(query, values)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        chunk_size: typing.Optional[int] = None,
        with_transaction: bool = True,
    ) -> typing.AsyncGenerator[interfaces.Record, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(
                query, values, chunk_size, with_transaction=with_transaction
            ):
                yield record

    async def batched_iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
        batch_wrapper: typing.Union[BatchCallable] = tuple,
        with_transaction: bool = True,
    ) -> typing.AsyncGenerator[BatchCallableResult, None]:
        async with self.connection() as connection:
            async for records in connection.batched_iterate(
                query, values, batch_size, with_transaction=with_transaction
            ):
                yield batch_wrapper(records)

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    async def run_sync(self, fn: typing.Callable, **kwargs: typing.Any) -> None:
        async with self.connection() as connection:
            return connection.run_sync(fn, **kwargs)

    async def create_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        async with self.connection() as connection:
            connection.create_all(meta, **kwargs)

    async def drop_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        async with self.connection() as connection:
            connection.drop_all(meta, **kwargs)

    def connection(self) -> "Connection":
        if self._global_connection is not None:
            return self._global_connection

        if not self._connection:
            self._connection = Connection(self, self.backend)
        return self._connection

    @contextlib.contextmanager
    def force_rollback(self) -> typing.Iterator[None]:
        initial = self._force_rollback
        self._force_rollback = True
        try:
            yield
        finally:
            self._force_rollback = initial

    @classmethod
    def get_backend(
        cls, scheme: str, overwrite_path="databasez.overwrites"
    ) -> interfaces.DatabaseBackend:
        scheme = scheme.replace("+", "_")
        database_class: typing.Type[interfaces.DatabaseBackend] = SQLAlchemyBackend
        connection_class: typing.Type[interfaces.ConnectionBackend] = SQLAlchemyConnection
        transaction_class: typing.Type[interfaces.TransactionBackend] = SQLAlchemyTransaction

        module: typing.Any = None
        try:
            module = importlib.import_module(f"{overwrite_path}.{scheme.replace('+', '_')}")
        except ImportError:
            if "+" in scheme:
                try:
                    module = importlib.import_module(f"{overwrite_path}.{scheme.split('+', 1)[0]}")
                except ImportError:
                    pass
        database_class = getattr(module, "Database", database_class)
        assert issubclass(database_class, interfaces.DatabaseBackend)
        connection_class = getattr(module, "Connection", connection_class)
        assert issubclass(connection_class, interfaces.ConnectionBackend)
        transaction_class = getattr(module, "Transaction", transaction_class)
        assert issubclass(transaction_class, interfaces.TransactionBackend)
        return database_class(
            connection_class=connection_class, transaction_class=transaction_class
        )
