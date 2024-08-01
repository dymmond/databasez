import asyncio
import gc
import os
from typing import MutableMapping

import pyodbc
import pytest
import sqlalchemy

from databasez import Database, DatabaseURL
from tests.shared_db import create_database_tables, drop_database_tables, notes

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]

if not any((x.endswith(" for SQL Server") for x in pyodbc.drivers())):
    DATABASE_URLS = list(filter(lambda x: "mssql" not in x, DATABASE_URLS))


@pytest.fixture(autouse=True, scope="function")
def create_test_database():
    # Create test databases
    for url in DATABASE_URLS:
        asyncio.run(create_database_tables(url))

    # Run the test suite
    yield

    for url in DATABASE_URLS:
        asyncio.run(drop_database_tables(url))


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_commit_on_root_transaction(database_url):
    """
    Because our tests are generally wrapped in rollback-isolation, they
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_transaction_rollback_low_level(database_url):
    """
    Ensure that an explicit `await transaction.rollback()` is supported.
    """

    async with Database(database_url) as database:
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
@pytest.mark.asyncio
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


# highly default isolation level specific


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.asyncio
async def test_transaction_context_sibling_task_isolation_example(database_url):
    """
    Ensure that transactions are running in sibling tasks are isolated from eachother.
    """
    # This is an practical example of the above test.
    db = Database(database_url)
    if db.url.dialect == "mssql":
        pytest.skip()
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
