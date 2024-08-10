import os

import pyodbc
import pytest

from databasez import Database, DatabaseURL
from databasez.testclient import DatabaseTestClient

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]

if not any((x.endswith(" for SQL Server") for x in pyodbc.drivers())):
    DATABASE_URLS = list(filter(lambda x: "mssql" not in x, DATABASE_URLS))


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_non_existing_normal(database_url):
    test_db = DatabaseTestClient(database_url)
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

    database = DatabaseTestClient(database_url, test_prefix="foobar")
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
        database_url, test_prefix="foobar", use_existing=True, drop_database=True
    )
    async with database2:
        async with database2.connection() as conn:
            # doesn't crash
            await conn.fetch_all("select * from FOOBAR")
    if database2.drop:
        assert not await database2.database_exists(database.test_db_url)
