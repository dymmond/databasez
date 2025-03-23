import contextlib
import platform
import sys

import pytest
import sqlalchemy

from databasez import Database

if not sys.platform.startswith("linux") and not sys.platform.startswith("win"):
    pytest.skip(
        "Unsupported system for jdbc test. Only Windows and Linux (with glibc) can execute the test.",
        allow_module_level=True,
    )

if platform.machine() not in {"aarch64", "x86_64"}:
    pytest.skip(
        f"Unsupported processor architecture for jdbc test: {platform.machine()}",
        allow_module_level=True,
    )


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)


@pytest.mark.asyncio
async def test_jdbc_connect():
    """
    Test basic connection
    """
    async with (
        Database(
            "jdbc+sqlite://testsuite.sqlite3?classpath=tests/sqlite-jdbc-3.47.0.0.jar&jdbc_driver=org.sqlite.JDBC",
        ) as database,
        database.connection(),
    ):
        pass


@pytest.mark.asyncio
async def test_dialect_quote():
    async with (
        Database(
            "jdbc+sqlite://testsuite.sqlite3?classpath=tests/sqlite-jdbc-3.47.0.0.jar&jdbc_driver=org.sqlite.JDBC"
        ) as database,
        database.connection() as connection,
    ):
        dialect = database.engine.dialect
        await connection.create_all(metadata)
        try:
            assert await connection.run_sync(dialect.has_table, "notes")
            assert not await connection.run_sync(dialect.has_table, "no'\"%tes")
            assert dialect.identifier_preparer.quote("ijfosisdfop") == "ijfosisdfop"
            assert dialect.identifier_preparer.quote("ijfos'i'sdfop") != "ijfos'i'sdfop"
        finally:
            await database.drop_all(metadata)


@pytest.mark.asyncio
async def test_jdbc_queries():
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``,
    `fetch_one()`, `iterate()` and `batched_iterate()` interfaces are all supported (using SQLAlchemy core).
    """
    async with (
        Database(
            "jdbc+sqlite://testsuite.sqlite3?classpath=tests/sqlite-jdbc-3.47.0.0.jar&jdbc_driver=org.sqlite.JDBC",
        ) as database,
        database.connection() as connection,
    ):
        await connection.create_all(metadata)
        try:
            async with connection.transaction(force_rollback=True):
                # execute()
                query = notes.insert()
                values = {"text": "example1", "completed": True}
                with contextlib.suppress(Exception):
                    await connection.execute(query, values)

                # execute_many()
                query = notes.insert()
                values = [
                    {"text": "example2", "completed": False},
                    {"text": "example3", "completed": True},
                ]
                await connection.execute_many(query, values)

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
        finally:
            await connection.drop_all(metadata)


@pytest.mark.asyncio
async def test_reflection():
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``,
    `fetch_one()`, `iterate()` and `batched_iterate()` interfaces are all supported (using SQLAlchemy core).
    """
    async with Database(
        "jdbc+sqlite://testsuite.sqlite3?classpath=tests/sqlite-jdbc-3.47.0.0.jar&jdbc_driver=org.sqlite.JDBC",
    ) as database:
        async with database.connection() as connection, connection.transaction() as transaction:
            await connection.create_all(metadata)
            await transaction.commit()
        try:
            # fetch_all()
            query = notes.select()
            assert await database.fetch_all(query=query) == []
            metadata_reflected = sqlalchemy.MetaData()
            await database.run_sync(metadata_reflected.reflect)
        finally:
            await database.drop_all(metadata)
    assert metadata_reflected.tables.keys()
    assert not metadata_reflected.tables["notes"].c.id.nullable
    assert (
        metadata_reflected.tables["notes"].c.id.type.as_generic().__class__ == sqlalchemy.Integer
    )
    assert (
        metadata_reflected.tables["notes"].c.text.type.as_generic().__class__ == sqlalchemy.String
    )
