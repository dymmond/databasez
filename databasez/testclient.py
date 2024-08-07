import asyncio
import os
import typing
from typing import Any

import nest_asyncio
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils.functions.database import _sqlite_file_exists
from sqlalchemy_utils.functions.orm import quote

from databasez import Database, DatabaseURL

nest_asyncio.apply()


async def _get_scalar_result(engine: typing.Any, sql: typing.Any) -> Any:
    try:
        async with engine.connect() as conn:
            return await conn.scalar(sql)
    except Exception:
        return False


class DatabaseTestClient(Database):
    """
    Client used only for unit testing.

    This client simply creates a "test_" from the database provided in the
    connection.
    """

    def __init__(
        self,
        url: typing.Union[str, "DatabaseURL", "sa.URL", Database],
        *,
        force_rollback: bool = False,
        use_existing: bool = False,
        drop_database: bool = False,
        test_prefix: str = "test_",
        **options: typing.Any,
    ):
        if isinstance(url, Database):
            test_database_url = (
                url.url.replace(database=f"{test_prefix}{url.database}")
                if test_prefix
                else url.url
            )
            # replace only if not cloning a DatabaseTestClient
            self.test_db_url = str(getattr(url, "test_db_url", test_database_url))
            self.use_existing = getattr(url, "use_existing", use_existing)
            self.drop = getattr(url, "drop", drop_database)
            asyncio.get_event_loop().run_until_complete(self.setup())
            super().__init__(url)
            # fix url
            if str(self.url) != self.test_db_url:
                self.url = test_database_url
        else:
            url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
            test_database_url = (
                url.replace(database=f"{test_prefix}{url.database}") if test_prefix else url
            )
            self.test_db_url = str(test_database_url)
            self.use_existing = use_existing
            self.drop = drop_database

            asyncio.get_event_loop().run_until_complete(self.setup())

            super().__init__(test_database_url, force_rollback=force_rollback, **options)

    async def setup(self) -> None:
        """
        Makes sure the database is created if does not exist or use existing
        if needed.
        """
        if not self.use_existing:
            if await self.database_exists(self.test_db_url):
                await self.drop_database(self.test_db_url)
            await self.create_database(self.test_db_url)
        else:
            if not await self.database_exists(self.test_db_url):
                await self.create_database(self.test_db_url)

    async def is_database_exist(self) -> Any:
        """
        Checks if a database exists.
        """
        return await self.database_exists(self.test_db_url)

    async def database_exists(self, url: typing.Union[str, "sa.URL", DatabaseURL]) -> Any:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect_name = url.sqla_url.get_dialect(True).name
        if dialect_name == "postgresql":
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, "postgres", "template1", "template0", None):
                url = url.replace(database=db)
                async with Database(url) as db_client:
                    try:
                        return bool(await _get_scalar_result(db_client.engine, sa.text(text)))
                    except (ProgrammingError, OperationalError):
                        pass
            return False

        elif dialect_name == "mysql":
            url = url.replace(database=None)
            text = (
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = '%s'" % database
            )
            async with Database(url) as db_client:
                return bool(await _get_scalar_result(db_client.engine, sa.text(text)))

        elif dialect_name == "sqlite":
            if database:
                return database == ":memory:" or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            text = "SELECT 1"
            async with Database(url) as db_client:
                try:
                    return bool(await _get_scalar_result(db_client.engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    return False

    async def create_database(
        self,
        url: typing.Union[str, "sa.URL", DatabaseURL],
        encoding: str = "utf8",
        template: typing.Any = None,
    ) -> Any:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect_name = url.sqla_url.get_dialect(True).name
        dialect_driver = url.sqla_url.get_dialect(True).driver

        if dialect_name == "postgresql":
            url = url.replace(database="postgres")
        elif dialect_name == "mssql":
            url = url.replace(database="master")
        elif dialect_name == "cockroachdb":
            url = url.replace(database="defaultdb")
        elif dialect_name != "sqlite":
            url = url.replace(database=None)

        if (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
            dialect_name == "postgresql"
            and dialect_driver in {"asyncpg", "pg8000", "psycopg", "psycopg2", "psycopg2cffi"}
        ):
            db_client = Database(url, isolation_level="AUTOCOMMIT")
        else:
            db_client = Database(url)
        await db_client.connect()

        if dialect_name == "postgresql":
            if not template:
                template = "template1"

            async with db_client.engine.begin() as conn:  # type: ignore
                text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
                    quote(conn, database), encoding, quote(conn, template)
                )
                await conn.execute(sa.text(text))

        elif dialect_name == "mysql":
            async with db_client.engine.begin() as conn:  # type: ignore
                text = "CREATE DATABASE {} CHARACTER SET = '{}'".format(
                    quote(conn, database), encoding
                )
                await conn.execute(sa.text(text))

        elif dialect_name == "sqlite" and database != ":memory:":
            if database:
                async with db_client.engine.begin() as conn:  # type: ignore
                    await conn.execute(sa.text("CREATE TABLE DB(id int)"))
                    await conn.execute(sa.text("DROP TABLE DB"))

        else:
            async with db_client.engine.begin() as conn:  # type: ignore
                text = f"CREATE DATABASE {quote(conn, database)}"
                await conn.execute(sa.text(text))

        await db_client.disconnect()

    async def drop_database(self, url: typing.Union[str, "sa.URL", DatabaseURL]) -> Any:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect_name = url.sqla_url.get_dialect(True).name
        dialect_driver = url.sqla_url.get_dialect(True).driver

        if dialect_name == "postgresql":
            url = url.replace(database="postgres")
        elif dialect_name == "mssql":
            url = url.replace(database="master")
        elif dialect_name == "cockroachdb":
            url = url.replace(database="defaultdb")
        elif dialect_name != "sqlite":
            url = url.replace(database=None)

        if (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
            dialect_name == "postgresql"
            and dialect_driver in {"asyncpg", "pg8000", "psycopg", "psycopg2", "psycopg2cffi"}
        ):
            db_client = Database(url, isolation_level="AUTOCOMMIT")
        else:
            db_client = Database(url)
        await db_client.connect()

        if dialect_name == "sqlite" and database != ":memory:":
            if database:
                os.remove(database)
        elif dialect_name == "postgresql":
            async with db_client.engine.begin() as conn:  # type: ignore
                # Disconnect all users from the database we are dropping.
                version = conn.dialect.server_version_info
                pid_column = "pid" if (version >= (9, 2)) else "procpid"  # type: ignore
                text = """
                SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{database}'
                AND {pid_column} <> pg_backend_pid();
                """.format(pid_column=pid_column, database=database)
                await conn.execute(sa.text(text))

                # Drop the database.
                text = f"DROP DATABASE {quote(conn, database)}"
                await conn.execute(sa.text(text))
        else:
            async with db_client.engine.begin() as conn:  # type: ignore
                text = f"DROP DATABASE {quote(conn, database)}"
                await conn.execute(sa.text(text))

        await db_client.disconnect()

    async def disconnect(self) -> None:
        if self.drop:
            await self.drop_database(self.test_db_url)
        await super().disconnect()
