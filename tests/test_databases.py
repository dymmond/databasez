import asyncio
import datetime
import decimal
import functools
import gc
import os
from typing import MutableMapping
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qsl, urlsplit

import pyodbc
import pytest
import sqlalchemy
from sqlalchemy.engine import URL, make_url

from databasez import Database, DatabaseURL

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
                    "scheme": spliter.scheme.split("+")[0],
                    "host": spliter.hostname,
                    "port": spliter.port,
                    "user": spliter.username,
                    "password": spliter.password,
                    "database": spliter.path[1:],
                    "options": dict(parse_qsl(spliter.query)),
                }
            }
        }
    )


MIXED_DATABASE_CONFIG_URLS = [*DATABASE_URLS, *DATABASE_CONFIG_URLS]
MIXED_DATABASE_CONFIG_URLS_IDS = [*DATABASE_URLS, *(f"{x}[config]" for x in DATABASE_URLS)]


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MyEpochType(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.Integer

    epoch = datetime.date(1970, 1, 1)

    def process_bind_param(self, value, dialect):
        return (value - self.epoch).days

    def process_result_value(self, value, dialect):
        return self.epoch + datetime.timedelta(days=value)


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)

# Used to test DateTime
articles = sqlalchemy.Table(
    "articles",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", sqlalchemy.DateTime),
)

# Used to test JSON
session = sqlalchemy.Table(
    "session",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("data", sqlalchemy.JSON),
)

# Used to test custom column types
custom_date = sqlalchemy.Table(
    "custom_date",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", MyEpochType),
)

# Used to test Numeric
prices = sqlalchemy.Table(
    "prices",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("price", sqlalchemy.Numeric(precision=30, scale=20)),
)


@pytest.fixture(autouse=True, scope="function")
def create_test_database():
    # Create test databases with tables creation
    for url in DATABASE_URLS:
        database_url = str(DatabaseURL(url))
        database_url = (
            database_url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )

        engine = sqlalchemy.create_engine(database_url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    # Drop test databases
    for url in DATABASE_URLS:
        database_url = str(DatabaseURL(url))
        database_url = (
            database_url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )

        engine = sqlalchemy.create_engine(database_url)
        metadata.drop_all(engine)


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.new_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_queries(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``,
    `fetch_one()`, `iterate()` and `batched_iterate()` interfaces are all supported (using SQLAlchemy core).
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # execute_many()
            query = notes.insert()
            values = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await database.execute_many(query, values)

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


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_queries_raw(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` interfaces are all supported (raw queries).
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_ddl_queries(database_url):
    """
    Test that the built-in DDL elements such as `DropTable()`,
    `CreateTable()` are supported (using SQLAlchemy core).
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            # DropTable()
            query = sqlalchemy.schema.DropTable(notes)
            await database.execute(query)

            # CreateTable()
            query = sqlalchemy.schema.CreateTable(notes)
            await database.execute(query)


@pytest.mark.parametrize("exception", [Exception, asyncio.CancelledError])
@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_queries_after_error(database_url, exception):
    """
    Test that the basic `execute()` works after a previous error.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_results_support_mapping_interface(database_url):
    """
    Casting results to a dict should work, since the interface defines them
    as supporting the mapping interface.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_result_values_allow_duplicate_names(database_url):
    """
    The values of a result should respect when two columns are selected
    with the same name.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            query = "SELECT 1 AS id, 2 AS id"
            row = await database.fetch_one(query=query)

            assert list(row._mapping.keys()) == ["id", "id"]
            assert list(row._mapping.values()) == [1, 2]


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_fetch_one_returning_no_results(database_url):
    """
    fetch_one should return `None` when no results match.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            # fetch_all()
            query = notes.select()
            result = await database.fetch_one(query=query)
            assert result is None


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_execute_return_val(database_url):
    """
    Test using return value from `execute()` to get an inserted primary key.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            pk = await database.execute(query, values)
            assert isinstance(pk, int)
            query = notes.select().where(notes.c.id == pk)
            result = await database.fetch_one(query)
            assert result.text == "example1"
            assert result.completed is True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_rollback_isolation(database_url):
    """
    Ensure that `database.transaction(force_rollback=True)` provides strict isolation.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        # Perform some INSERT operations on the database.
        async with database.transaction(force_rollback=True):
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

        # Ensure INSERT operations have been rolled back.
        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_rollback_isolation_with_contextmanager(database_url):
    """
    Ensure that `database.force_rollback()` provides strict isolation.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    database = Database(**data)

    with database.force_rollback():
        async with database:
            # Perform some INSERT operations on the database.
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

        async with database:
            # Ensure INSERT operations have been rolled back.
            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit(database_url):
    """
    Ensure that transaction commit is supported.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            async with database.transaction():
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit_serializable(database_url):
    """
    Ensure that serializable transaction commit via extra parameters is supported.
    """
    database_url = DatabaseURL(database_url)

    if database_url.scheme not in ["postgresql", "postgresql+asyncpg"]:
        pytest.skip("Test (currently) only supports asyncpg")

    def insert_independently():
        url = str(DatabaseURL(database_url))
        url = (
            url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )

        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()

        query = notes.insert().values(text="example1", completed=True)
        conn.execute(query)
        conn.close()

    def delete_independently():
        url = str(DatabaseURL(database_url))
        url = (
            url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()

        query = notes.delete()
        conn.execute(query)
        conn.close()

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True, isolation="serializable"):
            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0

            insert_independently()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0

            delete_independently()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_rollback(database_url):
    """
    Ensure that transaction rollback is supported.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            try:
                async with database.transaction():
                    query = notes.insert().values(text="example1", completed=True)
                    await database.execute(query)
                    raise RuntimeError()
            except RuntimeError:
                pass

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit_low_level(database_url):
    """
    Ensure that an explicit `await transaction.commit()` is supported.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            transaction = await database.transaction()
            try:
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)
            except Exception:
                await transaction.rollback()
            else:
                await transaction.commit()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1


@pytest.mark.parametrize("database_url", [DATABASE_URLS, DATABASE_CONFIG_URLS])
@async_adapter
async def test_transaction_rollback_low_level(database_url):
    """
    Ensure that an explicit `await transaction.rollback()` is supported.
    """
    database_url = database_url[0]
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.transaction(force_rollback=True):
            transaction = await database.transaction()
            try:
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)
                raise RuntimeError()
            except Exception:
                await transaction.rollback()
            else:  # pragma: no cover
                await transaction.commit()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_decorator(database_url):
    """
    Ensure that @database.transaction() is supported.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    database = Database(force_rollback=True, **data)

    @database.transaction()
    async def insert_data(raise_exception):
        query = notes.insert().values(text="example", completed=True)
        await database.execute(query)
        if raise_exception:
            raise RuntimeError()

    async with database:
        with pytest.raises(RuntimeError):
            await insert_data(raise_exception=True)

        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 0

        await insert_data(raise_exception=False)

        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_datetime_field(database_url):
    """
    Test DataTime columns, to ensure records are coerced to/from proper Python types.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
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
            if database_url.startswith("sqlite"):
                # aiosqlite does not support native decimals --> a round-off error is expected
                assert results[0].price == pytest.approx(price)
            else:
                assert results[0].price == price


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_json_field(database_url):
    """
    Test JSON columns, to ensure correct cross-database support.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
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


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_connections_isolation(database_url):
    """
    Ensure that changes are visible between different connections.
    To check this we have to not create a transaction, so that
    each query ends up on a different connection from the pool.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        try:
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
        finally:
            query = notes.delete()
            await database.execute(query)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_commit_on_root_transaction(database_url):
    """
    Because our tests are generally wrapped in rollback-islation, they
    don't have coverage for commiting the root transaction.

    Deal with this here, and delete the records rather than rolling back.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        try:
            async with database.transaction():
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
        finally:
            query = notes.delete()
            await database.execute(query)


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_connect_and_disconnect(database_url):
    """
    Test explicit connect() and disconnect().
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    database = Database(**data)

    assert not database.is_connected
    await database.connect()
    assert database.is_connected
    await database.disconnect()
    assert not database.is_connected

    # connect and disconnect idempotence
    await database.connect()
    await database.connect()
    assert database.is_connected
    await database.disconnect()
    await database.disconnect()
    assert not database.is_connected


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_connection_context(database_url):
    """
    Test connection contexts are task-local.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connection_context_with_raw_connection(database_url):
    """
    Test connection contexts with respect to the raw connection.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2
                assert connection_1.async_connection is connection_2.async_connection


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_queries_with_expose_backend_connection(database_url):
    """
    Replication of `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` using the raw driver interface.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        async with database.connection() as connection:
            async with connection.transaction(force_rollback=True):
                # Get the driver connection
                raw_connection = (await connection.get_raw_connection()).driver_connection
                # Insert query
                if database.url.scheme in [
                    "mysql",
                    "mysql+asyncmy",
                    "mysql+aiomysql",
                ]:
                    insert_query = "INSERT INTO notes (text, completed) VALUES (%s, %s)"
                else:
                    insert_query = "INSERT INTO notes (text, completed) VALUES (?, ?)"

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
                elif database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    await raw_connection.execute(insert_query, *values)
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
                elif database.url.scheme in ["mssql", "mssql+aioodbc", "mssql+pyodbc"]:
                    cursor = await raw_connection.cursor()
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

                assert len(results) == 3
                # Raw output for the raw request
                assert results[0][1] == "example1"
                assert results[0][2] == True
                assert results[1][1] == "example2"
                assert results[1][2] == False
                assert results[2][1] == "example3"
                assert results[2][2] == True

                # fetch_one()
                if database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    result = await raw_connection.fetchrow(select_query)
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


@pytest.mark.parametrize(
    "database_url", MIXED_DATABASE_CONFIG_URLS, ids=MIXED_DATABASE_CONFIG_URLS_IDS
)
@async_adapter
async def test_database_url_interface(database_url):
    """
    Test that Database instances expose a `.url` attribute.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        assert isinstance(database.url, DatabaseURL)
        if isinstance(database_url, str):
            assert database.url == database_url


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_concurrent_access_on_single_connection(database_url):
    database_url = DatabaseURL(database_url)
    if database_url.dialect != "postgresql":
        pytest.skip("Test requires `pg_sleep()`")

    async with Database(database_url, force_rollback=True) as database:

        async def db_lookup():
            await database.fetch_one("SELECT pg_sleep(1)")

        await asyncio.gather(db_lookup(), db_lookup())


@pytest.mark.parametrize("database_url", DATABASE_URLS)
def test_global_connection_is_initialized_lazily(database_url):
    """
    Ensure that global connection is initialized at latest possible time
    so it's _query_lock will belong to same event loop that async_adapter has
    initialized.

    See https://github.com/dymmond/databasez/issues/157 for more context.
    """

    database_url = DatabaseURL(database_url)
    if database_url.dialect != "postgresql":
        pytest.skip("Test requires `pg_sleep()`")

    database = Database(database_url, force_rollback=True)

    @async_adapter
    async def run_database_queries():
        async with database:

            async def db_lookup():
                await database.fetch_one("SELECT pg_sleep(1)")

            await asyncio.gather(db_lookup(), db_lookup())

    run_database_queries()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_outside_transaction_with_values(database_url):
    """
    Ensure `iterate()` works even without a transaction on all drivers.
    The asyncpg driver relies on server-side cursors without hold
    for iteration, which requires a transaction to be created.
    This is mentionned in both their documentation and their test suite.
    """

    database_url = DatabaseURL(database_url)
    if database_url.dialect == "mysql":
        pytest.skip("MySQL does not support `FROM (VALUES ...)` (F641)")

    async with Database(database_url) as database:
        if database_url.dialect == "mssql":
            query = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) as X(t)"
        else:
            query = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) as t"
        iterate_results = []

        async for result in database.iterate(query=query):
            iterate_results.append(result)

        assert len(iterate_results) == 5


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_outside_transaction_with_temp_table(database_url):
    """
    Same as test_iterate_outside_transaction_with_values but uses a
    temporary table instead of a list of values.
    """
    database_url = DatabaseURL(database_url)
    if database_url.dialect == "sqlite":
        pytest.skip("SQLite interface does not work with temporary tables.")

    async with Database(database_url) as database:
        if database_url.dialect == "mssql":
            query = "CREATE TABLE ##no_transac(num INTEGER)"
            await database.execute(query)

            query = "INSERT INTO ##no_transac VALUES (1), (2), (3), (4), (5)"
            await database.execute(query)

            query = "SELECT * FROM ##no_transac"

        else:
            query = "CREATE TEMPORARY TABLE no_transac(num INTEGER)"
            await database.execute(query)

            query = "INSERT INTO no_transac(num) VALUES (1), (2), (3), (4), (5)"
            await database.execute(query)

            query = "SELECT * FROM no_transac"

        iterate_results = []

        async for result in database.iterate(query=query):
            iterate_results.append(result)

        assert len(iterate_results) == 5


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.parametrize("select_query", [notes.select(), "SELECT * FROM notes"])
@async_adapter
async def test_column_names(database_url, select_query):
    """
    Test that column names are exposed correctly through `._mapping.keys()` on each row.
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_postcompile_queries(database_url):
    """
    Since SQLAlchemy 1.4, IN operators needs to do render_postcompile
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        await database.execute(query, values)

        query = notes.select().where(notes.c.id.in_([2, 3]))
        results = await database.fetch_all(query=query)

        assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_result_named_access(database_url):
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data, isolation_level="AUTOCOMMIT") as database:
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        result = await database.execute(query, values)
        assert result in {1, -1}
        result = await database.fetch_one(query=notes.select())
        assert result.text == "example1"
        assert result.completed is True

        query = notes.select().where(notes.c.text == "example1")
        result = await database.fetch_one(query=query)

        assert result.text == "example1"
        assert result.completed is True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_mapping_property_interface(database_url):
    """
    Test that all connections implement interface with `_mapping` property
    """
    if isinstance(database_url, str):
        data = {"url": database_url}
    else:
        data = {"config": database_url}

    async with Database(**data) as database:
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_child_task_inheritance(database_url):
    """
    Ensure that transactions are inherited by child tasks.
    """
    async with Database(database_url) as database:

        async def check_transaction(transaction, active_transaction):
            # Should have inherited the same transaction backend from the parent task
            assert transaction._transaction is active_transaction

        async with database.transaction() as transaction:
            await asyncio.create_task(check_transaction(transaction, transaction._transaction))


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_child_task_inheritance_example(database_url):
    """
    Ensure that child tasks may influence inherited transactions.
    """
    # This is an practical example of the above test.
    db = Database(database_url)
    if db.url.dialect == "mssql":
        return

    async with Database(database_url) as database:
        async with database.transaction():
            # Create a note
            await database.execute(notes.insert().values(id=1, text="setup", completed=True))

            # Change the note from the same task
            await database.execute(notes.update().where(notes.c.id == 1).values(text="prior"))

            # Confirm the change
            result = await database.fetch_one(notes.select().where(notes.c.id == 1))
            assert result.text == "prior"

            async def run_update_from_child_task(connection):
                # Change the note from a child task
                await connection.execute(notes.update().where(notes.c.id == 1).values(text="test"))

            await asyncio.create_task(run_update_from_child_task(database.connection()))

            # Confirm the child's change
            result = await database.fetch_one(notes.select().where(notes.c.id == 1))
            assert result.text == "test"


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_sibling_task_isolation(database_url):
    """
    Ensure that transactions are isolated between sibling tasks.
    """
    start = asyncio.Event()
    end = asyncio.Event()

    async with Database(database_url) as database:

        async def check_transaction(transaction):
            await start.wait()
            # Parent task is now in a transaction, we should not
            # see its transaction backend since this task was
            # _started_ in a context where no transaction was active.
            assert transaction._transaction is None
            end.set()

        transaction = database.transaction()
        assert transaction._transaction is None
        task = asyncio.create_task(check_transaction(transaction))

        async with transaction:
            start.set()
            assert transaction._transaction is not None
            await end.wait()

        # Cleanup for "Task not awaited" warning
        await task


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_sibling_task_isolation_example(database_url):
    """
    Ensure that transactions are running in sibling tasks are isolated from eachother.
    """
    # This is an practical example of the above test.
    db = Database(database_url)
    if db.url.dialect == "mssql":
        return
    setup = asyncio.Event()
    done = asyncio.Event()

    async def tx1(connection):
        async with connection.transaction():
            await db.execute(notes.insert(), values={"id": 1, "text": "tx1", "completed": False})
            setup.set()
            await done.wait()

    async def tx2(connection):
        async with connection.transaction():
            await setup.wait()
            result = await db.fetch_all(notes.select())
            assert result == [], result
            done.set()

    async with Database(database_url) as db:
        await asyncio.gather(tx1(db), tx2(db))


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connection_cleanup_contextmanager(database_url):
    """
    Ensure that task connections are not persisted unecessarily.
    """

    ready = asyncio.Event()
    done = asyncio.Event()

    async def check_child_connection(database: Database):
        async with database.connection():
            ready.set()
            await done.wait()

    async with Database(database_url) as database:
        # Should have a connection in this task
        # .connect is lazy, it doesn't create a Connection, but .connection does
        connection = database.connection()
        assert isinstance(database._connection_map, MutableMapping)
        assert database._connection_map.get(asyncio.current_task()) is connection

        # Create a child task and see if it registers a connection
        task = asyncio.create_task(check_child_connection(database))
        await ready.wait()
        assert database._connection_map.get(task) is not None
        assert database._connection_map.get(task) is not connection

        # Let the child task finish, and see if it cleaned up
        done.set()
        await task
        # This is normal exit logic cleanup, the WeakKeyDictionary
        # shouldn't have cleaned up yet since the task is still referenced
        assert task not in database._connection_map

    # Context manager closes, all open connections are removed
    assert isinstance(database._connection_map, MutableMapping)
    assert len(database._connection_map) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connection_cleanup_garbagecollector(database_url):
    """
    Ensure that connections for tasks are not persisted unecessarily, even
    if exit handlers are not called.
    """
    database = Database(database_url)
    await database.connect()

    created = asyncio.Event()

    async def check_child_connection(database: Database):
        # neither .disconnect nor .__aexit__ are called before deleting this task
        database.connection()
        created.set()

    task = asyncio.create_task(check_child_connection(database))
    await created.wait()
    assert task in database._connection_map
    await task
    del task
    gc.collect()

    # Should not have a connection for the task anymore
    assert len(database._connection_map) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_cleanup_contextmanager(database_url):
    """
    Ensure that contextvar transactions are not persisted unecessarily.
    """
    from databasez.core import ACTIVE_TRANSACTIONS

    assert ACTIVE_TRANSACTIONS.get() is None

    async with Database(database_url) as database:
        async with database.transaction() as transaction:
            open_transactions = ACTIVE_TRANSACTIONS.get()
            assert isinstance(open_transactions, MutableMapping)
            assert open_transactions.get(transaction) is transaction._transaction

        # Context manager closes, open_transactions is cleaned up
        open_transactions = ACTIVE_TRANSACTIONS.get()
        assert isinstance(open_transactions, MutableMapping)
        assert open_transactions.get(transaction, None) is None


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_context_cleanup_garbagecollector(database_url):
    """
    Ensure that contextvar transactions are not persisted unecessarily, even
    if exit handlers are not called.
    This test should be an XFAIL, but cannot be due to the way that is hangs
    during teardown.
    """
    from databasez.core import ACTIVE_TRANSACTIONS

    assert ACTIVE_TRANSACTIONS.get() is None

    async with Database(database_url) as database:
        transaction = database.transaction()
        await transaction.start()

        # Should be tracking the transaction
        open_transactions = ACTIVE_TRANSACTIONS.get()
        assert isinstance(open_transactions, MutableMapping)
        assert open_transactions.get(transaction) is transaction._transaction

        # neither .commit, .rollback, nor .__aexit__ are called
        del transaction
        gc.collect()

        # A strong reference to the transaction is kept alive by the connection's
        # ._transaction_stack, so it is still be tracked at this point.
        assert len(open_transactions) == 1

        # If that were magically cleared, the transaction would be cleaned up,
        # but as it stands this always causes a hang during teardown at
        # `Database(...).disconnect()` if the transaction is not closed.
        transaction = database.connection()._transaction_stack[-1]
        await transaction.rollback()
        del transaction

        # Now with the transaction rolled-back, it should be cleaned up.
        assert len(open_transactions) == 0
