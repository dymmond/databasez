"""
Forwarding implementations of interfaces to sqlalchemy.

Adjustments should be inherit from here.
"""

from __future__ import annotations

import logging
import typing
from itertools import islice

import orjson
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.sql import ClauseElement

from databasez.interfaces import ConnectionBackend, DatabaseBackend, Record, TransactionBackend

if typing.TYPE_CHECKING:
    from sqlalchemy import Connection

    from databasez.core import DatabaseURL

logger = logging.getLogger("databasez")

_P = typing.ParamSpec("_P")
_T = typing.TypeVar("_T", bound=typing.Any)


def batched(iterable: typing.Iterable[typing.Any], n: int) -> typing.Any:
    # dropin, batched is not available for pythpn < 3.12
    iterator = iter(iterable)
    batch = tuple(islice(iterator, n))
    while batch:
        yield batch
        batch = tuple(islice(iterator, n))


class SQLAlchemyTransaction(TransactionBackend):
    raw_transaction: typing.Optional[typing.Any] = None
    old_transaction_level: str = ""

    async def start(self, is_root: bool, **extra_options: typing.Any) -> None:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        assert self.raw_transaction is None, "Transaction is already initialized"
        in_transaction = connection.in_transaction()

        self.old_transaction_level = await connection.get_isolation_level()
        if "isolation_level" not in extra_options and not in_transaction:
            isolation_level = self.get_default_transaction_isolation_level(
                is_root, **extra_options
            )
            extra_options["isolation_level"] = isolation_level

        if (
            extra_options.get("isolation_level") is None
            or typing.cast(str, extra_options["isolation_level"]) == self.old_transaction_level
        ):
            extra_options.pop("isolation_level", None)
            self.old_transaction_level = ""
        if extra_options:
            await connection.execution_options(**extra_options)
        assert (
            await connection.get_isolation_level() != "AUTOCOMMIT"
        ), "transactions doesn't work with AUTOCOMMIT. Please specify another transaction level."
        if in_transaction:
            self.raw_transaction = await connection.begin_nested()
        else:
            self.raw_transaction = await connection.begin()

    async def _close(self) -> None:
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.close()
        connection = self.async_connection
        if connection is not None and self.old_transaction_level:
            await connection.execution_options(isolation_level=self.old_transaction_level)

    async def commit(self) -> None:
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.commit()
        await self._close()

    async def rollback(self) -> None:
        assert self.raw_transaction is not None, "Transaction is not initialized"
        await self.raw_transaction.rollback()
        await self._close()

    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: typing.Any
    ) -> typing.Optional[str]:
        return "SERIALIZABLE"


class SQLAlchemyConnection(ConnectionBackend):
    async_connection: typing.Optional[AsyncConnection] = None

    async def acquire(self) -> typing.Optional[typing.Any]:
        assert self.engine is not None, "Database is not started"
        assert self.async_connection is None, "Connection is already acquired"
        self.async_connection = await self.engine.connect()
        return self.async_connection.get_transaction()

    async def release(self) -> None:
        assert self.async_connection is not None, "Connection is not acquired"
        connection, self.async_connection = self.async_connection, None
        await connection.close()

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        with await self.execute_raw(query) as result:
            return typing.cast(typing.List[Record], result.fetchall())

    async def fetch_one(self, query: ClauseElement, pos: int = 0) -> typing.Optional[Record]:
        if pos > 0:
            query = query.offset(pos)
        if pos >= 0 and hasattr(query, "limit"):
            query = query.limit(1)
        with await self.execute_raw(query) as result:
            if pos >= 0:
                return typing.cast(typing.Optional[Record], result.first())
            elif pos == -1:
                return typing.cast(typing.Optional[Record], result.last())
            else:
                raise NotImplementedError(
                    f"Only positive numbers and -1 for the last result are currently supported: {pos}"
                )

    async def batched_iterate(
        self, query: ClauseElement, batch_size: typing.Optional[int] = None
    ) -> typing.AsyncGenerator[typing.Any, None]:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        database = self.database
        assert database
        if batch_size is None:
            batch_size = database.default_batch_size

        if not connection.dialect.supports_server_side_cursors:
            with await self.execute_raw(query) as result:
                for batch in batched(
                    typing.cast(typing.List[Record], result.fetchall()), batch_size
                ):
                    yield batch
                return

        async with (await connection.execution_options(yield_per=batch_size)).stream(  # type: ignore
            query
        ) as result:
            async for batch in result.partitions():
                yield batch

    async def execute_raw(self, stmt: typing.Any) -> typing.Any:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.execute(stmt)

    async def execute(self, stmt: typing.Any) -> int:
        """
        Executes statement and returns the last row id (query) or the row count of updates.

        Warning: can return -1 (e.g. psycopg) in case the result is unknown

        """
        with await self.execute_raw(stmt) as result:
            try:
                return typing.cast(int, result.lastrowid)
            except AttributeError:
                assert result.is_insert is False, "could not retrieve lastrowid"
                return typing.cast(int, result.rowcount)

    async def execute_many(self, stmts: typing.List[typing.Any]) -> None:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        for stmt in stmts:
            with await connection.execute(stmt):
                pass

    async def get_raw_connection(self) -> typing.Any:
        """The real raw connection."""
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return await connection.get_raw_connection()

    async def run_sync(
        self,
        fn: typing.Callable[typing.Concatenate[Connection, _P], _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return typing.cast(_T, await connection.run_sync(fn, *args, **kwargs))

    def in_transaction(self) -> bool:
        connection = self.async_connection
        assert connection is not None, "Connection is not acquired"
        return connection.in_transaction()


class SQLAlchemyDatabase(DatabaseBackend):
    default_isolation_level: typing.Optional[str] = "AUTOCOMMIT"
    default_batch_size: int = 100

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: typing.Any,
    ) -> typing.Tuple[DatabaseURL, typing.Dict[str, typing.Any]]:
        if self.default_isolation_level is not None:
            options.setdefault("isolation_level", self.default_isolation_level)
        new_query_options = dict(database_url.options)
        if "ssl" in new_query_options:
            assert "ssl" not in options
            ssl = typing.cast(str, new_query_options.pop("ssl"))
            options["ssl"] = {"true": True, "false": False}.get(ssl.lower(), ssl.lower())
        return database_url.replace(options=new_query_options), options

    def json_serializer(self, inp: dict) -> str:
        return orjson.dumps(inp).decode("utf8")

    def json_deserializer(self, inp: typing.Union[str, bytes]) -> dict:
        return typing.cast(dict, orjson.loads(inp))

    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        self.engine = create_async_engine(database_url.sqla_url, **options)

    async def disconnect(self) -> None:
        engine = self.engine
        assert engine is not None, "database is not initialized"
        await engine.dispose()