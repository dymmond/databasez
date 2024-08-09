import os

import pyodbc
import pytest

from databasez import Database
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
    test_db = DatabaseTestClient(database_url)
    async with DatabaseTestClient(test_db) as database:
        assert database.is_connected
    async with DatabaseTestClient(test_db, drop_database=True) as database:
        assert database.is_connected
