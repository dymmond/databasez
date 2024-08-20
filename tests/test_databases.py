import asyncio
import datetime
import decimal
import os
from collections.abc import Sequence
from unittest.mock import patch
from urllib.parse import parse_qsl, unquote, urlsplit

import pyodbc
import pytest
import sqlalchemy
from sqlalchemy.engine import URL, make_url

from databasez import Database, DatabaseURL
from tests.shared_db import (
    AsyncMock,
    articles,
    custom_date,
    database_client,
    notes,
    prices,
    session,
    stop_database_client,
)

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]

if not any((x.endswith(" for SQL Server") for x in pyodbc.drivers())):
    DATABASE_URLS = list(filter(lambda x: "mssql" not in x, DATABASE_URLS))

DATABASE_CONFIG_URLS = []
for value in DATABASE_URLS:
    url: URL = make_url(value)
    spliter = urlsplit(url.render_as_string(hide_password=False))
    DATABASE_CONFIG_URLS.append(
        {
            "connection": {
                "credentials": {
                    "scheme": spliter.scheme,
                    "host": spliter.hostname,
                    "port": spliter.port,
                    "user": spliter.username,
                    "password": unquote(spliter.password) if spliter.password else None,
                    "database": spliter.path[1:],
                    "options": dict(parse_qsl(spliter.query)),
                }
            }
        }
    )


MIXED_DATABASE_CONFIG_URLS = [*DATABASE_URLS, *DATABASE_CONFIG_URLS]
MIXED_DATABASE_CONFIG_URLS_IDS = [*DATABASE_URLS, *(f"{x}[config]" for x in DATABASE_URLS)]


@pytest.fixture(params=DATABASE_URLS)
def database_url(request):
    """Yield test database despite its name"""
    # yield test Databases
    loop = asyncio.new_event_loop()
    database = loop.run_until_complete(database_client(request.param))
    yield database
    loop.run_until_complete(stop_database_client(database))


@pytest.fixture(params=MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS)
def database_mixed_url(request):
    loop = asyncio.new_event_loop()
    database = loop.run_until_complete(database_client(request.param))
    yield request.param
    loop.run_until_complete(stop_database_client(database))


@pytest.mark.asyncio
async def test_queries(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``,
    `fetch_one()`, `iterate()` and `batched_iterate()` interfaces are all supported (using SQLAlchemy core).
    """

    async with Database(database_url, force_rollback=True) as database:
        # execute()
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        defaults = await database.execute(query, values)
        assert defaults.id == 1

        # execute_many()
        query = notes.insert()
        values = [
            {"text": "example2", "completed": False},
            {"text": "example3", "completed": True},
        ]
        defaults = await database.execute_many(query, values)
        assert defaults[0].id in {2, None}
        assert defaults[1].id in {3, None}

        # fetch_all()
        query = notes.select()
        results = await database.fetch_all(query=query)

        assert len(results) == 3
        assert results[0].text == "example1"
        assert results[0].completed is True
        assert results[1].text == "example2"
        assert results[1].completed is False
        assert results[2].text == "example3"
        assert results[2].completed is True

        # fetch_one()
        query = notes.select()
        result = await database.fetch_one(query=query)
        assert result.text == "example1"
        assert result.completed is True

        # fetch_val()
        query = sqlalchemy.sql.select(*[notes.c.text])
        result = await database.fetch_val(query=query)
        assert result == "example1"

        # fetch_val() with no rows
        query = sqlalchemy.sql.select(*[notes.c.text]).where(notes.c.text == "impossible")
        result = await database.fetch_val(query=query)
        assert result is None

        # fetch_val() with a different column
        query = sqlalchemy.sql.select(*[notes.c.id, notes.c.text])
        result = await database.fetch_val(query=query, column=1)
        assert result == "example1"

        # row access (needed to maintain test coverage for Record.__getitem__ in postgres backend)
        query = sqlalchemy.sql.select(*[notes.c.text])
        result = await database.fetch_one(query=query)
        assert result.text == "example1"
        assert result[0] == "example1"

        # iterate()
        query = notes.select()
        iterate_results = []
        async for result in database.iterate(query=query):
            iterate_results.append(result)
        assert len(iterate_results) == 3
        assert iterate_results[0].text == "example1"
        assert iterate_results[0].completed is True
        assert iterate_results[1].text == "example2"
        assert iterate_results[1].completed is False
        assert iterate_results[2].text == "example3"
        assert iterate_results[2].completed is True

        # batched_iterate()
        query = notes.select()
        batched_iterate_results = []
        async for result in database.batched_iterate(query=query, batch_size=2):
            batched_iterate_results.append(result)
        assert len(batched_iterate_results) == 2
        assert batched_iterate_results[0][0].text == "example1"
        assert batched_iterate_results[0][0].completed is True
        assert batched_iterate_results[0][1].text == "example2"
        assert batched_iterate_results[0][1].completed is False
        assert batched_iterate_results[1][0].text == "example3"
        assert batched_iterate_results[1][0].completed is True


@pytest.mark.asyncio
async def test_queries_raw(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` interfaces are all supported (raw queries).
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = "INSERT INTO notes(text, completed) VALUES (:text, :completed)"
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # execute_many()
            query = "INSERT INTO notes(text, completed) VALUES (:text, :completed)"
            values = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await database.execute_many(query, values)

            # fetch_all()
            query = "SELECT * FROM notes WHERE completed = :completed"
            results = await database.fetch_all(query=query, values={"completed": True})
            assert len(results) == 2
            assert results[0].text == "example1"
            assert results[0].completed == True
            assert results[1].text == "example3"
            assert results[1].completed == True

            # fetch_one()
            query = "SELECT * FROM notes WHERE completed = :completed"
            result = await database.fetch_one(query=query, values={"completed": False})
            assert result.text == "example2"
            assert result.completed == False

            # fetch_val()
            query = "SELECT completed FROM notes WHERE text = :text"
            result = await database.fetch_val(query=query, values={"text": "example1"})
            assert result == True

            query = "SELECT * FROM notes WHERE text = :text"
            result = await database.fetch_val(
                query=query, values={"text": "example1"}, column="completed"
            )
            assert result == True

            # iterate()
            query = "SELECT * FROM notes"
            iterate_results = []
            async for result in database.iterate(query=query):
                iterate_results.append(result)
            assert len(iterate_results) == 3
            assert iterate_results[0].text == "example1"
            assert iterate_results[0].completed == True
            assert iterate_results[1].text == "example2"
            assert iterate_results[1].completed == False
            assert iterate_results[2].text == "example3"
            assert iterate_results[2].completed == True


@pytest.mark.asyncio
async def test_ddl_queries(database_url):
    """
    Test that the built-in DDL elements such as `DropTable()`,
    `CreateTable()` are supported (using SQLAlchemy core).
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # DropTable()
            query = sqlalchemy.schema.DropTable(notes)
            await database.execute(query)

            # CreateTable()
            query = sqlalchemy.schema.CreateTable(notes)
            await database.execute(query)


@pytest.mark.asyncio
async def test_connection_after_iterate(database_url):
    """
    Test that the connection is correctly resetted.
    """

    async with Database(database_url, force_rollback=True) as database:
        # execute() before iterate
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        defaults = await database.execute(query, values)
        assert defaults.id == 1
        query = "SELECT * FROM notes"
        results = []
        async for result in database.iterate(query=query, chunk_size=2):
            results.append(result)
        assert len(results) == 1

        # execute() after iterate
        query = notes.insert()
        values = {"text": "after_iterate", "completed": True}
        defaults = await database.execute(query, values)


@pytest.mark.asyncio
async def test_connection_after_batched_iterate(database_url):
    """
    Test that the connection is correctly resetted.
    """

    async with Database(database_url, force_rollback=True) as database:
        # execute() before batched_iterate
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        defaults = await database.execute(query, values)
        assert defaults.id == 1
        query = "SELECT * FROM notes"
        results = []
        async for result in database.batched_iterate(query=query, batch_size=2):
            results.append(result)
        assert len(results) == 1
        assert len(results[0]) == 1

        # execute() after batched_iterate
        query = notes.insert()
        values = {"text": "after_iterate", "completed": True}
        defaults = await database.execute(query, values)


@pytest.mark.parametrize("exception", [Exception, asyncio.CancelledError])
@pytest.mark.asyncio
async def test_queries_after_error(database_url, exception):
    """
    Test that the basic `execute()` works after a previous error.
    """

    async with Database(database_url) as database:
        with patch.object(
            database.connection()._connection,
            "acquire",
            new=AsyncMock(side_effect=exception),
        ):
            with pytest.raises(exception):
                query = notes.select()
                await database.fetch_all(query)

        query = notes.select()
        await database.fetch_all(query)


@pytest.mark.asyncio
async def test_results_support_mapping_interface(database_url):
    """
    Casting results to a dict should work, since the interface defines them
    as supporting the mapping interface.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # fetch_all()
            query = notes.select()
            results = await database.fetch_all(query=query)
            results_as_dicts = [dict(item._mapping) for item in results]

            assert len(results[0]) == 3
            assert len(results_as_dicts[0]) == 3

            assert isinstance(results_as_dicts[0]["id"], int)
            assert results_as_dicts[0]["text"] == "example1"
            assert results_as_dicts[0]["completed"] is True


@pytest.mark.asyncio
async def test_result_values_allow_duplicate_names(database_url):
    """
    The values of a result should respect when two columns are selected
    with the same name.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            query = "SELECT 1 AS id, 2 AS id"
            row = await database.fetch_one(query=query)

            assert list(row._mapping.keys()) == ["id", "id"]
            assert list(row._mapping.values()) == [1, 2]


@pytest.mark.asyncio
async def test_fetch_one_returning_no_results(database_url):
    """
    fetch_one should return `None` when no results match.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # fetch_all()
            query = notes.select()
            result = await database.fetch_one(query=query)
            assert result is None


@pytest.mark.asyncio
async def test_execute_return_val(database_url):
    """
    Test using return value from `execute()` to get an inserted primary key.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            pk1 = await database.execute(query, values)
            if isinstance(pk1, Sequence):
                pk1 = pk1[0]
            values = {"text": "example2", "completed": True}
            pk2 = await database.execute(query, values)
            if isinstance(pk2, Sequence):
                pk2 = pk2[0]
            assert isinstance(pk1, int) and pk1 > 0
            query = notes.select().where(notes.c.id == pk1)
            result = await database.fetch_one(query)
            assert result.text == "example1"
            assert result.completed is True
            assert isinstance(pk2, int) and pk2 > 0
            query = notes.select().where(notes.c.id == pk2)
            result = await database.fetch_one(query)
            assert result.text == "example2"
            assert result.completed is True


@pytest.mark.asyncio
async def test_datetime_field(database_url):
    """
    Test DataTime columns, to ensure records are coerced to/from proper Python types.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            now = datetime.datetime.now().replace(microsecond=0)

            # execute()
            query = articles.insert()
            values = {"title": "Hello, world", "published": now}
            await database.execute(query, values)

            # fetch_all()
            query = articles.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0].title == "Hello, world"
            assert results[0].published == now


@pytest.mark.asyncio
async def test_decimal_field(database_url):
    """
    Test Decimal (NUMERIC) columns, to ensure records are coerced to/from proper Python types.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            price = decimal.Decimal("0.700000000000001")

            # execute()
            query = prices.insert()
            values = {"price": price}
            await database.execute(query, values)

            # fetch_all()
            query = prices.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            if str(database.url).startswith("sqlite"):
                # aiosqlite does not support native decimals --> a round-off error is expected
                assert results[0].price == pytest.approx(price)
            else:
                assert results[0].price == price


@pytest.mark.asyncio
async def test_json_field(database_url):
    """
    Test JSON columns, to ensure correct cross-database support.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            data = {"text": "hello", "boolean": True, "int": 1}
            values = {"data": data}
            query = session.insert()
            await database.execute(query, values)

            # fetch_all()
            query = session.select()
            results = await database.fetch_all(query=query)

            assert len(results) == 1
            assert results[0].data == {"text": "hello", "boolean": True, "int": 1}


@pytest.mark.asyncio
async def test_custom_field(database_url):
    """
    Test custom column types.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            today = datetime.date.today()

            # execute()
            query = custom_date.insert()
            values = {"title": "Hello, world", "published": today}

            await database.execute(query, values)

            # fetch_all()
            query = custom_date.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0].title == "Hello, world"
            assert results[0].published == today


@pytest.mark.asyncio
async def test_connections_isolation(database_url):
    """
    Ensure that changes are visible between different connections.
    To check this we have to not create a transaction, so that
    each query ends up on a different connection from the pool.
    """
    async with Database(database_url) as database:
        try:
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
        finally:
            query = notes.delete()
            await database.execute(query)


@pytest.mark.asyncio
async def test_connect_and_disconnect(database_mixed_url):
    """
    Test explicit connect() and disconnect().
    """
    if isinstance(database_mixed_url, str):
        data = {"url": database_mixed_url}
    else:
        data = {"config": database_mixed_url}

    database = Database(**data)

    assert not database.is_connected
    assert not database.force_rollback
    assert database.engine is None
    assert database.ref_counter == 0
    assert await database.connect()
    assert database.is_connected
    assert database.engine is not None
    assert database.ref_counter == 1

    # copy
    copied_db = database.__copy__()
    assert copied_db.ref_counter == 0
    assert not database.force_rollback
    assert not copied_db.is_connected
    assert copied_db.engine is None

    # second method
    copied_db = Database(database, force_rollback=True)
    assert copied_db.ref_counter == 0
    assert not copied_db.is_connected
    assert copied_db.engine is None
    assert copied_db.force_rollback

    copied_db2 = copied_db.__copy__()
    assert copied_db2.force_rollback

    old_engine = database.engine
    assert database.ref_counter == 1
    assert await database.disconnect()
    assert not database.is_connected
    assert database.ref_counter == 0

    # connect and disconnect refcounting
    assert await database.connect()
    assert database.engine is not old_engine
    assert database.is_connected
    old_engine = database.engine
    # nest
    assert not await database.connect()
    assert database.ref_counter == 2
    assert database.engine is old_engine
    assert database.is_connected
    assert not await database.disconnect()
    assert database.ref_counter == 1
    await database.disconnect()
    assert database.ref_counter == 0
    assert not database.is_connected
    assert not database._global_connection


@pytest.mark.asyncio
async def test_force_rollback(database_url):
    async with Database(database_url, force_rollback=False) as database:
        assert not database.force_rollback
        database.force_rollback = True
        assert database.force_rollback
        # execute()
        data = {"text": "hello", "boolean": True, "int": 2}
        values = {"data": data}
        query = session.insert()
        await database.execute(query, values)
        async with database.connection() as connection_1:
            assert connection_1 is database._global_connection
        with database.force_rollback(False):
            assert not database.force_rollback
            async with database.connection() as connection_2:
                assert connection_2 is not database._global_connection
                if database.url.dialect != "sqlite":
                    # sqlite has locking problems
                    data = {"text": "hello", "boolean": True, "int": 1}
                    values = {"data": data}
                    query = session.insert()
                    await database.execute(query, values)

        # now reset
        del database.force_rollback
        assert not database.force_rollback

    async with Database(database_url, force_rollback=True) as database:
        # fetch_all()
        query = session.select()
        results = await database.fetch_all(query=query)
        if database.url.dialect == "sqlite":
            assert len(results) == 0
        else:
            assert len(results) == 1
            assert results[0].data == {"text": "hello", "boolean": True, "int": 1}


@pytest.mark.asyncio
async def test_connection_context(database_url):
    """
    Test connection contexts are task-local.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2

    async with Database(database_url) as database:
        connection_1 = None
        connection_2 = None
        test_complete = asyncio.Event()

        async def get_connection_1():
            nonlocal connection_1

            async with database.connection() as connection:
                connection_1 = connection
                await test_complete.wait()

        async def get_connection_2():
            nonlocal connection_2

            async with database.connection() as connection:
                connection_2 = connection
                await test_complete.wait()

        loop = asyncio.get_event_loop()
        task_1 = loop.create_task(get_connection_1())
        task_2 = loop.create_task(get_connection_2())
        while connection_1 is None or connection_2 is None:
            await asyncio.sleep(0.000001)
        assert connection_1 is not connection_2
        test_complete.set()
        await task_1
        await task_2


@pytest.mark.asyncio
async def test_connection_context_with_raw_connection(database_url):
    """
    Test connection contexts with respect to the raw connection.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2
                assert connection_1.async_connection is connection_2.async_connection


@pytest.mark.asyncio
async def test_queries_with_expose_backend_connection(database_url):
    """
    Replication of `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` using the raw driver interface.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection:
            async with connection.transaction(force_rollback=True):
                # Get the driver connection
                raw_connection = (await connection.get_raw_connection()).driver_connection
                # Insert query
                if database.url.scheme in [
                    "mysql",
                    "mysql+asyncmy",
                    "mysql+aiomysql",
                    "postgresql+psycopg",
                ]:
                    insert_query = r"INSERT INTO notes (text, completed) VALUES (%s, %s)"
                elif database.url.scheme == "postgresql+asyncpg":
                    insert_query = r"INSERT INTO notes (text, completed) VALUES ($1, $2)"
                else:
                    insert_query = r"INSERT INTO notes (text, completed) VALUES (?, ?)"

                # execute()
                values = ("example1", True)

                if database.url.scheme in [
                    "mysql",
                    "mysql+aiomysql",
                    "mssql",
                    "mssql+pyodbc",
                    "mssql+aioodbc",
                ]:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(insert_query, values)
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(insert_query, values)
                elif database.url.scheme in [
                    "postgresql",
                    "postgresql+asyncpg",
                ]:
                    await raw_connection.execute(insert_query, *values)
                elif database.url.scheme in [
                    "postgresql+psycopg",
                ]:
                    await raw_connection.execute(insert_query, values)
                elif database.url.scheme in ["sqlite", "sqlite+aiosqlite"]:
                    await raw_connection.execute(insert_query, values)

                # execute_many()
                values = [("example2", False), ("example3", True)]

                if database.url.scheme in ["mysql", "mysql+aiomysql"]:
                    cursor = await raw_connection.cursor()
                    await cursor.executemany(insert_query, values)
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.executemany(insert_query, values)
                elif database.url.scheme in [
                    "mssql",
                    "mssql+aioodbc",
                    "mssql+pyodbc",
                ]:
                    cursor = await raw_connection.cursor()
                    for value in values:
                        await cursor.execute(insert_query, value)
                elif database.url.scheme in ["postgresql+psycopg"]:
                    cursor = raw_connection.cursor()
                    for value in values:
                        await cursor.execute(insert_query, value)
                else:
                    await raw_connection.executemany(insert_query, values)

                # Select query
                select_query = "SELECT notes.id, notes.text, notes.completed FROM notes"

                # fetch_all()
                if database.url.scheme in [
                    "mysql",
                    "mysql+aiomysql",
                    "mssql",
                    "mssql+pyodbc",
                    "mssql+aioodbc",
                ]:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(select_query)
                        results = await cursor.fetchall()
                elif database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    results = await raw_connection.fetch(select_query)
                elif database.url.scheme in ["sqlite", "sqlite+aiosqlite"]:
                    results = await raw_connection.execute_fetchall(select_query)
                elif database.url.scheme == "postgresql+psycopg":
                    cursor = raw_connection.cursor()
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()

                assert len(results) == 3
                # Raw output for the raw request
                assert results[0][1] == "example1"
                assert results[0][2] == True
                assert results[1][1] == "example2"
                assert results[1][2] == False
                assert results[2][1] == "example3"
                assert results[2][2] == True

                # fetch_one()
                if database.url.scheme in [
                    "postgresql",
                    "postgresql+asyncpg",
                ]:
                    result = await raw_connection.fetchrow(select_query)
                elif database.url.scheme in [
                    "postgresql+psycopg",
                ]:
                    cursor = raw_connection.cursor()
                    await cursor.execute(select_query)
                    result = await cursor.fetchone()
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(select_query)
                        result = await cursor.fetchone()
                elif database.url.scheme in ["mssql", "mssql+pyodbc", "mssql+aioodbc"]:
                    cursor = await raw_connection.cursor()
                    try:
                        await cursor.execute(select_query)
                        result = await cursor.fetchone()
                    finally:
                        await cursor.close()
                else:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    result = await cursor.fetchone()

                # Raw output for the raw request
                assert result[1] == "example1"
                assert result[2] == True


@pytest.mark.asyncio
async def test_database_url_interface(database_mixed_url):
    """
    Test that Database instances expose a `.url` attribute.
    """
    if isinstance(database_mixed_url, str):
        data = {"url": database_mixed_url}
    else:
        data = {"config": database_mixed_url}

    async with Database(**data) as database:
        assert isinstance(database.url, DatabaseURL)
        if isinstance(database_mixed_url, str):
            assert database.url == database_mixed_url


def _startswith(tested, params):
    for param in params:
        if tested.startswith(param):
            return True
    return False


@pytest.mark.asyncio
async def test_concurrent_access_on_single_connection(database_url):
    database_url = DatabaseURL(str(database_url.url))
    if not _startswith(database_url.dialect, ["mysql", "mariadb", "postgres", "mssql"]):
        pytest.skip("Test requires sleep function")
    async with Database(database_url, force_rollback=True) as database:

        async def db_lookup():
            if database_url.dialect.startswith("postgres"):
                await database.fetch_one("SELECT pg_sleep(0.3)")
            elif database_url.dialect.startswith("mysql") or database_url.dialect.startswith(
                "mariadb"
            ):
                await database.fetch_one("SELECT SLEEP(0.3)")
            elif database_url.dialect.startswith("mssql"):
                await database.execute("WAITFOR DELAY '00:00:00.300'")

        await asyncio.gather(db_lookup(), db_lookup(), db_lookup())


@pytest.mark.parametrize("force_rollback", [True, False])
@pytest.mark.asyncio
async def test_multi_thread(database_url, force_rollback):
    database_url = DatabaseURL(str(database_url.url))
    async with Database(database_url, force_rollback=force_rollback) as database:
        database._non_copied_attribute = True

        async def db_lookup(in_thread):
            async with database.connection() as conn:
                if in_thread:
                    assert not hasattr(conn._database, "_non_copied_attribute")
                else:
                    assert hasattr(conn._database, "_non_copied_attribute")
                assert bool(conn._database.force_rollback) == force_rollback
            if not _startswith(database_url.dialect, ["mysql", "mariadb", "postgres", "mssql"]):
                return
            if database_url.dialect.startswith("postgres"):
                await database.fetch_one("SELECT pg_sleep(0.3)")
            elif database_url.dialect.startswith("mysql") or database_url.dialect.startswith(
                "mariadb"
            ):
                await database.fetch_one("SELECT SLEEP(0.3)")
            elif database_url.dialect.startswith("mssql"):
                await database.execute("WAITFOR DELAY '00:00:00.300'")

        async def wrap_in_thread():
            await asyncio.to_thread(asyncio.run, db_lookup(True))

        await asyncio.gather(db_lookup(False), wrap_in_thread(), wrap_in_thread())


@pytest.mark.asyncio
async def test_multi_thread_db_contextmanager(database_url):
    async with Database(database_url, force_rollback=False) as database:

        async def db_connect(depth=3):
            # many parallel and nested threads
            async with database as new_database:
                await new_database.fetch_one("SELECT 1")
                ops = []
                while depth >= 0:
                    depth -= 1
                    ops.append(asyncio.to_thread(asyncio.run, db_connect(depth=depth)))
                await asyncio.gather(*ops)
            assert new_database.ref_counter == 0

        await asyncio.to_thread(asyncio.run, db_connect())
    assert database.ref_counter == 0


@pytest.mark.asyncio
async def test_multi_thread_db_connect_fails(database_url):
    async with Database(database_url, force_rollback=True) as database:

        async def db_connect():
            await database.connect()

        with pytest.raises(RuntimeError):
            await asyncio.to_thread(asyncio.run, db_connect())


@pytest.mark.asyncio
async def test_global_connection_is_initialized_lazily(database_url):
    """
    Ensure that global connection is initialized at latest possible time
    so it's _query_lock will belong to same event loop that async_adapter has
    initialized.

    See https://github.com/dymmond/databasez/issues/157 for more context.
    """

    database_url = DatabaseURL(database_url.url)
    if not _startswith(database_url.dialect, ["mysql", "mariadb", "postgres", "mssql"]):
        pytest.skip("Test requires sleep function")

    database = Database(database_url, force_rollback=True)

    async def run_database_queries():
        async with database:

            async def db_lookup():
                if database_url.dialect.startswith("postgres"):
                    await database.fetch_one("SELECT pg_sleep(0.3)")
                elif database_url.dialect.startswith("mysql") or database_url.dialect.startswith(
                    "mariadb"
                ):
                    await database.fetch_one("SELECT SLEEP(0.3)")
                elif database_url.dialect.startswith("mssql"):
                    await database.execute("WAITFOR DELAY '00:00:00.300'")

            await asyncio.gather(db_lookup(), db_lookup(), db_lookup())

    await run_database_queries()
    await database.disconnect()


@pytest.mark.parametrize("select_query", [notes.select(), "SELECT * FROM notes"])
@pytest.mark.asyncio
async def test_column_names(database_url, select_query):
    """
    Test that column names are exposed correctly through `._mapping.keys()` on each row.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # insert values
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)
            # fetch results
            results = await database.fetch_all(query=select_query)
            assert len(results) == 1

            assert sorted(results[0]._mapping.keys()) == ["completed", "id", "text"]
            assert results[0].text == "example1"
            assert results[0].completed == True


@pytest.mark.asyncio
async def test_result_named_access(database_url):
    async with Database(database_url) as database:
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        result = await database.execute(query, values)
        if isinstance(result, Sequence):
            result = result[0]
        assert result in {1, -1}
        result = await database.fetch_one(query=notes.select())
        assert result.text == "example1"
        assert result.completed is True

        query = notes.select().where(notes.c.text == "example1")
        result = await database.fetch_one(query=query)

        assert result.text == "example1"
        assert result.completed is True


@pytest.mark.asyncio
async def test_mapping_property_interface(database_url):
    """
    Test that all connections implement interface with `_mapping` property
    """
    async with Database(database_url) as database:
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        await database.execute(query, values)

        query = notes.select()
        single_result = await database.fetch_one(query=query)
        assert single_result._mapping["text"] == "example1"
        assert single_result._mapping["completed"] is True

        list_result = await database.fetch_all(query=query)
        assert list_result[0]._mapping["text"] == "example1"
        assert list_result[0]._mapping["completed"] is True
