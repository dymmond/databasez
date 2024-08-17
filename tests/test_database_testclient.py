import os

import pyodbc
import pytest

from databasez import Database, DatabaseURL
from databasez.testclient import DatabaseTestClient

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]

if not any((x.endswith(" for SQL Server") for x in pyodbc.drivers())):
    DATABASE_URLS = list(filter(lambda x: "mssql" not in x, DATABASE_URLS))


class LazyTestClient(DatabaseTestClient):
    testclient_default_lazy_setup: bool = True


# BaseExceptions are harder to catch so test against them
class HookException(BaseException):
    pass


class FailingConnectTestClient(LazyTestClient):
    async def connect_hook(self) -> None:
        raise HookException()


class FailingDisconnectTestClient(LazyTestClient):
    async def connect_hook(self) -> None:
        pass

    async def disconnect_hook(self) -> None:
        raise HookException()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_non_existing_normal(database_url):
    test_db = DatabaseTestClient(database_url, lazy_setup=True)
    async with Database(test_db) as database:
        assert database.is_connected
    async with Database(test_db) as database:
        assert database.is_connected


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_non_existing_client(database_url):
    db_url = DatabaseURL(database_url)
    async with DatabaseTestClient(database_url) as database:
        assert database.is_connected
    database = DatabaseTestClient(
        database_url, use_existing=True, drop_database=db_url.dialect != "mssql"
    )
    await database.connect()
    assert database.is_connected
    await database.disconnect()
    # could have been disabled, so check drop
    if database.drop:
        assert not await database.database_exists(database.test_db_url)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_client_drop_existing(database_url):
    db_url = DatabaseURL(database_url)
    db_url = db_url.replace(database=f"foobar{db_url.database}")
    if db_url.dialect not in {"sqlite", "postgresql", "postgres"}:
        pytest.skip("not supported")
        return
    # cleanup
    if await DatabaseTestClient.database_exists(str(db_url)):
        await DatabaseTestClient.drop_database(str(db_url))
    assert not await DatabaseTestClient.database_exists(str(db_url))
    # check if created
    assert not await DatabaseTestClient.database_exists(str(db_url))
    del db_url

    database = DatabaseTestClient(database_url, test_prefix="foobar", lazy_setup=True)
    assert not await DatabaseTestClient.database_exists(database.test_db_url)
    await database.connect()
    assert await database.database_exists(database.test_db_url)
    assert database.drop is False
    async with database.connection() as conn:
        await conn.execute("CREATE TABLE FOOBAR (id INTEGER NOT NULL, PRIMARY KEY(id))")
        # doesn't crash
        await conn.fetch_all("select * from FOOBAR")
    await database.disconnect()
    assert await database.database_exists(database.test_db_url)
    database2 = DatabaseTestClient(
        database_url, test_prefix="foobar", use_existing=True, drop_database=True, lazy_setup=True
    )
    async with database2:
        async with database2.connection() as conn:
            # doesn't crash
            await conn.fetch_all("select * from FOOBAR")
    if database2.drop:
        assert not await database2.database_exists(database.test_db_url)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_client_overwrite_defaults(database_url):
    class DummyTestClient(LazyTestClient):
        testclient_default_use_existing = True
        testclient_default_drop_database = True
        testclient_default_test_prefix = "foobar123"

    database = DummyTestClient(database_url)
    assert "foobar123" in database.test_db_url
    assert database.use_existing is True
    assert database.drop is True
    # lazy setup
    assert database._setup_executed_init is False
    assert not await database.database_exists(database.test_db_url)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.parametrize("source_db_class", [Database, LazyTestClient])
def test_client_copy(source_db_class, database_url):
    ref_db = Database(database_url)
    source_db = source_db_class(database_url, force_rollback=True)
    copied = DatabaseTestClient(source_db, lazy_setup=True)
    assert copied.url.database == f"test_{ref_db.url.database}"


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_client_fails_on_connect_hook(database_url):
    database = FailingConnectTestClient(database_url)
    with pytest.raises(HookException):
        async with database:
            pass
    assert database.is_connected == False
    # return True, would connect again
    assert await database.inc_refcount()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_client_fails_on_disconnect_hook(database_url):
    database = FailingDisconnectTestClient(database_url)
    with pytest.raises(HookException):
        async with database:
            assert database.is_connected == True
    assert database.is_connected == False
    # return True, would connect again
    assert await database.inc_refcount()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_client_fails_during_op(database_url):
    # mssql cannot drop master db
    database = LazyTestClient(database_url, use_existing=True)
    with pytest.raises(HookException):
        async with database:
            raise HookException()
    assert database.is_connected == False
