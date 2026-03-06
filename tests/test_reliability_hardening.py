from __future__ import annotations

import asyncio
import sys
from concurrent.futures import Future
from types import ModuleType
from unittest.mock import AsyncMock

import pytest

from databasez import Database, DatabaseURL, utils
from databasez.dialects.dbapi2 import DBAPI2_dialect
from databasez.dialects.jdbc import JDBC_dialect
from databasez.overwrites.postgresql import Database as PostgreSQLOverwriteDatabase
from databasez.sqlalchemy import SQLAlchemyConnection, SQLAlchemyTransaction
from databasez.testclient import DatabaseTestClient


@pytest.mark.asyncio
async def test_disconnect_does_not_underflow_when_not_connected() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    assert await database.disconnect() is False
    assert database.ref_counter == 0


@pytest.mark.asyncio
async def test_force_disconnect_resets_refcount() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    assert await database.connect() is True
    assert await database.connect() is False
    assert database.ref_counter == 2

    assert await database.disconnect(force=True) is True
    assert database.ref_counter == 0
    assert database.is_connected is False

    # Ensure force-disconnect leaves the instance reusable.
    assert await database.connect() is True
    assert database.ref_counter == 1
    assert await database.disconnect() is True


@pytest.mark.asyncio
async def test_double_commit_raises_and_does_not_underflow_connection_counter() -> None:
    async with Database("sqlite+aiosqlite:///:memory:") as database:
        connection = database.connection()
        transaction = await connection.transaction()
        await transaction.commit()

        assert connection._connection_counter == 0
        with pytest.raises(RuntimeError, match="not active"):
            await transaction.commit()
        assert connection._connection_counter == 0


@pytest.mark.asyncio
async def test_transaction_cleanup_on_commit_error() -> None:
    async with Database("sqlite+aiosqlite:///:memory:") as database:
        connection = database.connection()
        transaction = await connection.transaction()
        backend_transaction = transaction._transaction
        backend_transaction.commit = AsyncMock(side_effect=RuntimeError("commit exploded"))

        with pytest.raises(RuntimeError, match="commit exploded"):
            await transaction.commit()

        assert connection._connection_counter == 0
        assert connection._transaction_stack == []


@pytest.mark.asyncio
async def test_transaction_cleanup_on_rollback_error() -> None:
    async with Database("sqlite+aiosqlite:///:memory:") as database:
        connection = database.connection()
        transaction = await connection.transaction()
        backend_transaction = transaction._transaction
        backend_transaction.rollback = AsyncMock(side_effect=RuntimeError("rollback exploded"))

        with pytest.raises(RuntimeError, match="rollback exploded"):
            await transaction.rollback()

        assert connection._connection_counter == 0
        assert connection._transaction_stack == []


@pytest.mark.asyncio
async def test_out_of_order_transaction_commit_is_rejected() -> None:
    async with Database("sqlite+aiosqlite:///:memory:") as database:
        connection = database.connection()
        outer = await connection.transaction()
        inner = await connection.transaction()

        with pytest.raises(RuntimeError, match="active transaction"):
            await outer.commit()

        await inner.rollback()
        await outer.rollback()
        assert connection._connection_counter == 0


@pytest.mark.asyncio
async def test_arun_coroutine_threadsafe_cancels_future_on_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future: Future[object] = Future()

    class FakeLoop:
        def is_running(self) -> bool:
            return True

        def is_closed(self) -> bool:
            return False

    async def never_finishes(_future: Future[object], _loop: FakeLoop, _poll: float) -> object:
        await asyncio.sleep(10)
        return object()

    def fake_run_coroutine_threadsafe(coro: object, _loop: FakeLoop) -> Future[object]:
        coro.close()  # avoid "coroutine was never awaited" warnings
        return future

    monkeypatch.setattr(utils, "_arun_coroutine_threadsafe_result_shim", never_finishes)
    monkeypatch.setattr(utils.asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    task = asyncio.create_task(
        utils.arun_coroutine_threadsafe(asyncio.sleep(0), FakeLoop(), poll_interval=0.001)
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert future.cancelled()


def test_postgresql_overwrite_rewrites_psycopg2_to_async_driver() -> None:
    backend = PostgreSQLOverwriteDatabase(SQLAlchemyConnection, SQLAlchemyTransaction)
    rewritten_url, _ = backend.extract_options(
        DatabaseURL("postgresql+psycopg2://user:pass@localhost/test_db")
    )
    assert rewritten_url.driver == "psycopg"


def test_charset_name_sanitization_blocks_sql_injection_payloads() -> None:
    with pytest.raises(ValueError):
        DatabaseTestClient._sanitize_charset_name("utf8'; DROP DATABASE prod; --")


def test_setup_protected_raises_on_timeout() -> None:
    class SlowSetupClient(DatabaseTestClient):
        async def setup(self) -> None:
            await asyncio.sleep(0.2)

    database = SlowSetupClient("sqlite+aiosqlite:///:memory:", lazy_setup=True, test_prefix="")
    with pytest.raises(TimeoutError):
        database.setup_protected(0.01)


def test_dbapi2_has_table_uses_identifier_not_string_literal() -> None:
    dialect = DBAPI2_dialect()

    class FakeConnection:
        statement: str = ""

        def execute(self, statement: object) -> None:
            self.statement = str(statement)

    connection = FakeConnection()
    assert dialect.has_table(connection, "notes") is True
    assert "from '" not in connection.statement.lower()


def test_jdbc_get_indexes_uses_each_index_sort_order(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSQLException(Exception):
        pass

    java_module = ModuleType("java")
    java_sql_module = ModuleType("java.sql")
    java_sql_module.SQLException = FakeSQLException
    java_module.sql = java_sql_module
    monkeypatch.setitem(sys.modules, "java", java_module)
    monkeypatch.setitem(sys.modules, "java.sql", java_sql_module)

    class FakeResultSet:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows
            self._index = -1

        def next(self) -> bool:
            self._index += 1
            return self._index < len(self._rows)

        def getShort(self, name: str) -> int:
            return int(self._rows[self._index][name])

        def getString(self, name: str) -> str | None:
            value = self._rows[self._index][name]
            return None if value is None else str(value)

        def getBoolean(self, name: str) -> bool:
            return bool(self._rows[self._index][name])

        def close(self) -> None:
            return None

    class FakeMetaData:
        def getSearchStringEscape(self) -> str:
            return "\\"

        def getIndexInfo(
            self,
            _catalog: object,
            _schema: str,
            _table_name: str,
            _unique: bool,
            _approximate: bool,
        ) -> FakeResultSet:
            return FakeResultSet(
                [
                    {
                        "ORDINAL_POSITION": 1,
                        "INDEX_NAME": "idx_a",
                        "NON_UNIQUE": False,
                        "COLUMN_NAME": "id",
                        "ASC_OR_DESC": "A",
                    },
                    {
                        "ORDINAL_POSITION": 1,
                        "INDEX_NAME": "idx_b",
                        "NON_UNIQUE": False,
                        "COLUMN_NAME": "id",
                        "ASC_OR_DESC": "D",
                    },
                ]
            )

    class FakeJDBCConnection:
        def getMetaData(self) -> FakeMetaData:
            return FakeMetaData()

    import databasez.dialects.jdbc as jdbc_module

    monkeypatch.setattr(
        jdbc_module, "unpack_to_jdbc_connection", lambda _connection: FakeJDBCConnection()
    )

    dialect = JDBC_dialect()
    indexes = dialect.get_indexes(connection=object(), table_name="test_table")
    sort_order_by_name = {
        index["name"]: index["column_sorting"]["id"]
        for index in indexes
        if index["name"] is not None
    }

    assert sort_order_by_name["idx_a"] == ("asc",)
    assert sort_order_by_name["idx_b"] == ("desc",)
