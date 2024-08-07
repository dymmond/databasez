from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import typing
import weakref
from functools import lru_cache
from types import TracebackType

from databasez import interfaces

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
    url: DatabaseURL
    options: typing.Any

    def __init__(
        self,
        url: typing.Optional[typing.Union[str, DatabaseURL, URL, Database]] = None,
        *,
        force_rollback: bool = False,
        config: typing.Optional["DictAny"] = None,
        **options: typing.Any,
    ):
        init()
        assert config is None or url is None, "Use either 'url' or 'config', not both."
        if isinstance(url, Database):
            assert not options, "Cannot specify options when copying a Database object."
            self.backend = url.backend.__copy__()
            self.url = url.url
            self.options = url.options
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
        self.backend.owner = self
        self.is_connected = False
        self._connection_map = weakref.WeakKeyDictionary()

        self._force_rollback = force_rollback
        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection: typing.Optional[Connection] = None
        self._global_transaction: typing.Optional[Transaction] = None

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
        pos: int = 0,
    ) -> typing.Optional[interfaces.Record]:
        async with self.connection() as connection:
            return await connection.fetch_one(query, values)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
        pos: int = 0,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.fetch_val(query, values, column=column, pos=pos)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Any = None,
    ) -> typing.Union[interfaces.Record, int]:
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
    ) -> typing.AsyncGenerator[interfaces.Record, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(query, values, chunk_size):
                yield record

    async def batched_iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
        batch_wrapper: typing.Union[BatchCallable] = tuple,
    ) -> typing.AsyncGenerator[BatchCallableResult, None]:
        async with self.connection() as connection:
            async for records in connection.batched_iterate(query, values, batch_size):
                yield batch_wrapper(records)

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    async def run_sync(
        self,
        fn: typing.Callable[..., typing.Any],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.run_sync(fn, *args, **kwargs)

    async def create_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        async with self.connection() as connection:
            await connection.create_all(meta, **kwargs)

    async def drop_all(self, meta: MetaData, **kwargs: typing.Any) -> None:
        async with self.connection() as connection:
            await connection.drop_all(meta, **kwargs)

    def connection(self) -> Connection:
        if self._global_connection is not None:
            return self._global_connection

        if not self._connection:
            self._connection = Connection(self, self.backend)
        return self._connection

    @property
    def engine(self) -> typing.Optional[AsyncEngine]:
        return self.backend.engine

    @contextlib.contextmanager
    def force_rollback(self) -> typing.Iterator[None]:
        initial = self._force_rollback
        self._force_rollback = True
        try:
            yield
        finally:
            self._force_rollback = initial

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
