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
        force_rollback: typing.Union[bool, None] = None,
        use_existing: bool = False,
        drop_database: bool = False,
        test_prefix: str = "test_",
        **options: typing.Any,
    ):
        if isinstance(url, Database):
            test_database_url = (
                url.url.replace(database=f"{test_prefix}{url.url.database}")
                if test_prefix
                else url.url
            )
            # replace only if not cloning a DatabaseTestClient
            self.test_db_url = str(getattr(url, "test_db_url", test_database_url))
            self.use_existing = getattr(url, "use_existing", use_existing)
            self.drop = getattr(url, "drop", drop_database)
            super().__init__(url, force_rollback=force_rollback, **options)
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

            super().__init__(test_database_url, force_rollback=force_rollback, **options)

    async def connect_hook(self) -> None:
        """
        Makes sure the database is created if does not exist or use existing
        if needed.
        """
        db_does_exist = await self.database_exists(self.test_db_url)
        if not self.use_existing:
            try:
                if db_does_exist:
                    await self.drop_database(self.test_db_url)
                await self.create_database(self.test_db_url)
            except (ProgrammingError, OperationalError, TypeError):
                self.drop = False
        else:
            if not db_does_exist:
                try:
                    await self.create_database(self.test_db_url)
                except (ProgrammingError, OperationalError):
                    self.drop = False
        await super().connect_hook()

    async def is_database_exist(self) -> Any:
        """
        Checks if a database exists.
        """
        return await self.database_exists(self.test_db_url)

    @classmethod
    async def database_exists(cls, url: typing.Union[str, "sa.URL", DatabaseURL]) -> bool:
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

    @classmethod
    async def create_database(
        cls,
        url: typing.Union[str, "sa.URL", DatabaseURL],
        encoding: str = "utf8",
        template: typing.Any = None,
    ) -> None:
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
            db_client = Database(url, isolation_level="AUTOCOMMIT", force_rollback=False)
        else:
            db_client = Database(url, force_rollback=False)
        async with db_client:
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

    @classmethod
    async def drop_database(cls, url: typing.Union[str, "sa.URL", DatabaseURL]) -> None:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect = url.sqla_url.get_dialect(True)
        dialect_name = dialect.name
        dialect_driver = dialect.driver

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
            db_client = Database(url, isolation_level="AUTOCOMMIT", force_rollback=False)
        else:
            db_client = Database(url, force_rollback=False)
        async with db_client:
            if dialect_name == "sqlite" and database and database != ":memory:":
                try:
                    os.remove(database)
                except FileNotFoundError:
                    pass
            elif dialect_name.startswith("postgres"):
                async with db_client.connection() as conn:
                    # Disconnect all users from the database we are dropping.
                    server_version_raw = (
                        await conn.fetch_val(
                            "SELECT setting FROM pg_settings WHERE name = 'server_version'"
                        )
                    ).split(" ")[0]
                    version = tuple(map(int, server_version_raw.split(".")))
                    pid_column = "pid" if (version >= (9, 2)) else "procpid"
                    quoted_db = quote(conn.async_connection, database)
                    text = """
                    SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{database}'
                    AND {pid_column} <> pg_backend_pid();
                    """.format(pid_column=pid_column, database=quoted_db)
                    await conn.execute(text)

                    # Drop the database.
                    text = f"DROP DATABASE {quoted_db}"
                    try:
                        await conn.execute(text)
                    except ProgrammingError:
                        pass
            else:
                async with db_client.connection() as conn:
                    text = f"DROP DATABASE {quote(conn.async_connection, database)}"
                    await conn.execute(sa.text(text))

    async def disconnect_hook(self) -> None:
        if self.drop:
            try:
                await self.drop_database(self.test_db_url)
            except (ProgrammingError, OperationalError):
                pass
        await super().disconnect_hook()
