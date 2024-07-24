from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import typing
import weakref
from contextvars import ContextVar
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, quote, unquote, urlencode, urlsplit

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.sql import ClauseElement

from databasez import interfaces
from databasez.sqlalchemy import SQLAlchemyBackend, SQLAlchemyConnection, SQLAlchemyTransaction

if typing.TYPE_CHECKING:
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


ACTIVE_TRANSACTIONS: ContextVar[
    typing.Optional[weakref.WeakKeyDictionary[Transaction, interfaces.TransactionBackend]]
] = ContextVar("ACTIVE_TRANSACTIONS", default=None)


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
        self.url = url  # type: ignore
        self.options = options
        self.is_connected = False
        self._connection_map = weakref.WeakKeyDictionary()

        self._force_rollback = force_rollback

        self._backend = SQLAlchemyBackend(
            connection_class=SQLAlchemyConnection, transaction_class=SQLAlchemyTransaction
        )

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

        await self._backend.connect(self.url, **self.options)
        logger.info("Connected to database %s", self.url.obscure_password, extra=CONNECT_EXTRA)
        self.is_connected = True

        if self._force_rollback:
            assert self._global_connection is None
            assert self._global_transaction is None

            self._global_connection = Connection(self, self._backend)
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

        await self._backend.disconnect()
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

    def connection(self) -> "Connection":
        if self._global_connection is not None:
            return self._global_connection

        if not self._connection:
            self._connection = Connection(self, self._backend)
        return self._connection

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    @contextlib.contextmanager
    def force_rollback(self) -> typing.Iterator[None]:
        initial = self._force_rollback
        self._force_rollback = True
        try:
            yield
        finally:
            self._force_rollback = initial

    def _get_backend(self) -> str:
        return self.SUPPORTED_BACKENDS.get(
            self.url.scheme, self.SUPPORTED_BACKENDS[self.url.dialect]
        )


class Connection:
    def __init__(self, database: Database, backend: interfaces.DatabaseBackend) -> None:
        self._database = database
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: typing.List[Transaction] = []

        self._query_lock = asyncio.Lock()

    async def __aenter__(self) -> Connection:
        async with self._connection_lock:
            self._connection_counter += 1
            try:
                if self._connection_counter == 1:
                    await self._connection.acquire()
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
    ) -> typing.Optional[interfaces.Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
    ) -> typing.Any:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_val(built_query, column)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> int:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.execute(built_query)

    async def execute_many(self, query: typing.Union[ClauseElement, str], values: list) -> None:
        queries = [self._build_query(query, values_set) for values_set in values]
        async with self._query_lock:
            await self._connection.execute_many(queries)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
        with_transaction: bool = True,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            if with_transaction:
                async with self.transaction():
                    async for record in self._connection.iterate(built_query, batch_size):
                        yield record
            else:
                async for record in self._connection.iterate(built_query, batch_size):
                    yield record

    async def batched_iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        batch_size: typing.Optional[int] = None,
        with_transaction: bool = True,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            if with_transaction:
                async with self.transaction():
                    async for records in self._connection.batched_iterate(built_query, batch_size):
                        yield records
            else:
                async for records in self._connection.batched_iterate(built_query, batch_size):
                    yield records

    def transaction(self, *, force_rollback: bool = False, **kwargs: typing.Any) -> "Transaction":
        def connection_callable() -> Connection:
            return self

        return Transaction(connection_callable, force_rollback, **kwargs)

    @property
    def raw_connection(self) -> typing.Any:
        return self._connection.raw_connection

    @staticmethod
    def _build_query(
        query: typing.Union[ClauseElement, str], values: typing.Optional[dict] = None
    ) -> ClauseElement:
        if isinstance(query, str):
            query = text(query)

            return query.bindparams(**values) if values is not None else query
        elif values:
            return query.values(**values)  # type: ignore

        return query


_CallableType = typing.TypeVar("_CallableType", bound=typing.Callable)


class Transaction:
    def __init__(
        self,
        connection_callable: typing.Callable[[], Connection],
        force_rollback: bool,
        **kwargs: typing.Any,
    ) -> None:
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs

    @property
    def _connection(self) -> Connection:
        # Returns the same connection if called multiple times
        return self._connection_callable()

    @property
    def _transaction(self) -> typing.Optional[interfaces.TransactionBackend]:
        transactions = ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            return None

        return transactions.get(self, None)

    @_transaction.setter
    def _transaction(
        self, transaction: typing.Optional[interfaces.TransactionBackend]
    ) -> typing.Optional[interfaces.TransactionBackend]:
        transactions = ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            transactions = weakref.WeakKeyDictionary()
        else:
            transactions = transactions.copy()

        if transaction is None:
            transactions.pop(self, None)
        else:
            transactions[self] = transaction

        ACTIVE_TRANSACTIONS.set(transactions)
        return transactions.get(self, None)

    async def __aenter__(self) -> Transaction:
        """
        Called when entering `async with database.transaction()`
        """
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> typing.Generator[None, None, Transaction]:
        """
        Called if using the low-level `transaction = await database.transaction()`
        """
        return self.start().__await__()

    def __call__(self, func: _CallableType) -> _CallableType:
        """
        Called if using `@database.transaction()` as a decorator.
        """

        @functools.wraps(func)
        async def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper  # type: ignore

    async def start(self) -> Transaction:
        self._transaction = self._connection._connection.transaction()

        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            await self._connection.__aenter__()
            await self._transaction.start(is_root=is_root, extra_options=self._extra_options)
            self._connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            assert self._transaction is not None
            await self._transaction.commit()
            await self._transaction.close()
            await self._connection.__aexit__()
            self._transaction = None

    async def rollback(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            assert self._transaction is not None
            await self._transaction.rollback()
            await self._transaction.close()
            await self._connection.__aexit__()
            self._transaction = None


class _EmptyNetloc(str):
    def __bool__(self) -> bool:
        return True


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL", None] = None):
        if isinstance(url, DatabaseURL):
            self._url: str = url._url
        elif isinstance(url, str):
            self._url = url
        elif url is None:
            self._url = "invalid://localhost"
        else:
            raise TypeError(
                f"Invalid type for DatabaseURL. Expected str or DatabaseURL, got {type(url)}"
            )

    @classmethod
    def upgrade_scheme(cls, scheme: str) -> str:
        if "+" not in scheme:
            if scheme.startswith("postgres"):
                return "postgresql+psycopg"
            if scheme == "sqlite":
                return "sqlite+aiosqlite"
            if scheme == "mssql":
                return "mssql+aioodbc"
            if scheme == "mysql":
                return "mysql+asyncmy"
        return scheme

    @property
    def components(self) -> SplitResult:
        if not hasattr(self, "_components"):
            url = make_url(self._url)
            self.password = url.password
            _components = urlsplit(url.render_as_string(hide_password=False))
            # upgrade, regardless if scheme has an upgrade
            _components._replace(scheme=self.upgrade_scheme(_components.scheme))
            if _components.path.startswith("///"):
                _components = _components._replace(path=f"//{_components.path.lstrip('/')}")
            self._components = _components
        return self._components

    @classmethod
    def get_url(cls, splitted: SplitResult) -> str:
        url = f"{cls.upgrade_scheme(splitted.scheme)}://{splitted.netloc}{splitted.path}"
        if splitted.query:
            url = f"{url}?{splitted.query}"
        if splitted.fragment:
            url = f"{url}#{splitted.fragment}"
        return url

    @property
    def scheme(self) -> str:
        return self.upgrade_scheme(self.components.scheme)

    @property
    def dialect(self) -> str:
        return self.scheme.split("+")[0]

    @property
    def driver(self) -> str:
        return self.scheme.split("+", 1)[1]

    @property
    def userinfo(self) -> typing.Optional[bytes]:
        if self.components.username:
            info = self.components.username
            if self.password:
                info += ":" + quote(self.password)
            return info.encode("utf-8")
        return None

    @property
    def username(self) -> typing.Optional[str]:
        if self.components.username is None:
            return None
        return unquote(self.components.username)

    @property
    def password(self) -> typing.Optional[str]:
        if self.components.password is None and getattr(self, "_password", None) is None:
            return None

        if getattr(self, "_password", None) is None:
            return self.components.password
        return typing.cast(str, self._password)

    @password.setter
    def password(self, value: typing.Any) -> None:
        self._password = value

    @property
    def hostname(self) -> typing.Optional[str]:
        return (
            self.components.hostname or self.options.get("host") or self.options.get("unix_sock")
        )

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return unquote(path)

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if (
            "username" in kwargs
            or "user" in kwargs
            or "password" in kwargs
            or "hostname" in kwargs
            or "host" in kwargs
            or "port" in kwargs
        ):
            hostname = kwargs.pop("hostname", kwargs.pop("host", self.hostname))
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", kwargs.pop("user", self.components.username))
            password = kwargs.pop("password", self.components.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            # pathes should begin with /
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if "scheme" in kwargs:
            kwargs["scheme"] = self.upgrade_scheme(kwargs["scheme"])

        if not kwargs.get("netloc", self.netloc):
            # Using an empty string that evaluates as True means we end up
            # with URLs like `sqlite:///database` instead of `sqlite:/database`
            kwargs["netloc"] = _EmptyNetloc()
        if "options" in kwargs:
            kwargs["query"] = kwargs.pop("options")

        components = self.components._replace(**kwargs)
        return self.__class__(self.get_url(components))

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password="********")._url
        return str(self)

    def __str__(self) -> str:
        return self.get_url(self.components)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: typing.Any) -> bool:
        return str(self) == str(other)
