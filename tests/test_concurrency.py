import asyncio
import contextvars
import functools
import os
from concurrent.futures import Future
from threading import Thread

import pyodbc
import pytest

from databasez import Database, DatabaseURL
from tests.shared_db import (
    database_client,
    notes,
    stop_database_client,
)

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]

if not any((x.endswith(" for SQL Server") for x in pyodbc.drivers())):
    DATABASE_URLS = list(filter(lambda x: "mssql" not in x, DATABASE_URLS))


try:
    to_thread = asyncio.to_thread
except AttributeError:
    # for py <= 3.8
    async def to_thread(func, /, *args, **kwargs):
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)


@pytest.fixture(params=DATABASE_URLS)
def database_url(request):
    """Yield test database despite its name"""
    # yield test Databases
    loop = asyncio.new_event_loop()
    database = loop.run_until_complete(database_client(request.param))
    yield database
    loop.run_until_complete(stop_database_client(database))


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

        async def db_lookup(in_thread):
            async with database.connection() as conn:
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
            await to_thread(asyncio.run, db_lookup(True))

        await asyncio.gather(db_lookup(False), wrap_in_thread(), wrap_in_thread())


def _future_helper(awaitable, future):
    try:
        future.set_result(asyncio.run(awaitable))
    except BaseException as exc:
        future.set_exception(exc)


@pytest.mark.parametrize(
    "join_type",
    ["to_thread"],  # , "thread_join_with_context", "thread_join_without_context"]
)
@pytest.mark.parametrize("force_rollback", [True, False])
@pytest.mark.asyncio
async def test_multi_thread_db_contextmanager(database_url, force_rollback, join_type):
    async with Database(database_url, force_rollback=force_rollback) as database:
        query = notes.insert().values(text="examplecontext", completed=True)
        await database.execute(query, timeout=10)
        database._non_copied_attribute = True

        async def db_connect(depth=3):
            # many parallel and nested threads
            async with database as new_database:
                assert not hasattr(new_database, "_non_copied_attribute")
                query = notes.select()
                result = await database.fetch_one(query)
                assert result.text == "examplecontext"
                assert result.completed is True
                # test delegate to sub database
                assert database.engine is new_database.engine
                # also this shouldn't fail because redirected
                old_refcount = new_database.ref_counter
                await database.connect()
                assert new_database.ref_counter == old_refcount + 1
                await database.disconnect()
                ops = []
                while depth >= 0:
                    depth -= 1
                    ops.append(to_thread(asyncio.run, db_connect(depth=depth)))
                await asyncio.gather(*ops)
            assert new_database.ref_counter == 0

        if join_type.startswith("thread_join"):
            future = Future()
            args = [_future_helper, asyncio.wait_for(db_connect(), 5), future]
            if join_type == "thread_join_with_context":
                ctx = contextvars.copy_context()
                args.insert(0, ctx.run)
            thread = Thread(target=args[0], args=args[1:])
            thread.start()
            future.result()
        else:
            await to_thread(asyncio.run, asyncio.wait_for(db_connect(), 5))
    assert database.ref_counter == 0
    if force_rollback:
        async with database:
            query = notes.select()
            result = await database.fetch_one(query)
            assert result is None


@pytest.mark.asyncio
async def test_multi_thread_db_connect(database_url):
    async with Database(database_url, force_rollback=True) as database:

        async def db_connect():
            await database.connect()
            await database.fetch_one("SELECT 1")
            await database.disconnect()

        await to_thread(asyncio.run, db_connect())


@pytest.mark.asyncio
async def test_multi_thread_db_fails(database_url):
    async with Database(database_url, force_rollback=True) as database:

        async def db_connect():
            # not in same loop
            database.disconnect()

        with pytest.raises(RuntimeError):
            await to_thread(asyncio.run, db_connect())


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
