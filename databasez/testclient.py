import asyncio
import contextlib
import os
from typing import Any, Union

import sqlalchemy
from sqlalchemy.exc import OperationalError, ProgrammingError

from databasez import Database, DatabaseURL
from databasez.utils import DATABASEZ_POLL_INTERVAL, ThreadPassingExceptions, get_quoter


class DatabaseTestClient(Database):
    """
    Client used only for unit testing.

    This client simply creates a "test_" from the database provided in the
    connection.
    """

    # knob for changing the timeout of the setup and tear down of the db
    testclient_operation_timeout: float = 4
    testclient_operation_timeout_init: float = 8
    # is used for copying Database and DatabaseTestClientand providing an early url
    test_db_url: str
    # hooks for overwriting defaults of args with None
    testclient_default_full_isolation: bool = True
    testclient_default_force_rollback: bool = False
    testclient_default_poll_interval: float = DATABASEZ_POLL_INTERVAL
    testclient_default_lazy_setup: bool = False
    # customization hooks
    testclient_default_use_existing: bool = False
    testclient_default_drop_database: bool = False
    testclient_default_test_prefix: str = "test_"

    def __init__(
        self,
        url: Union[str, DatabaseURL, sqlalchemy.URL, Database, None] = None,
        *,
        force_rollback: Union[bool, None] = None,
        full_isolation: Union[bool, None] = None,
        poll_interval: Union[float, None] = None,
        use_existing: Union[bool, None] = None,
        drop_database: Union[bool, None] = None,
        lazy_setup: Union[bool, None] = None,
        test_prefix: Union[str, None] = None,
        **options: Any,
    ):
        if use_existing is None:
            use_existing = self.testclient_default_use_existing
        if drop_database is None:
            drop_database = self.testclient_default_drop_database
        if full_isolation is None:
            full_isolation = self.testclient_default_full_isolation
        if test_prefix is None:
            test_prefix = self.testclient_default_test_prefix
        self._setup_executed_init = False
        if isinstance(url, Database):
            self.use_existing = getattr(url, "use_existing", use_existing)
            self.drop = getattr(url, "drop", drop_database)
            # only if explicit set to False
            if lazy_setup is False:
                self.setup_protected(self.testclient_operation_timeout_init)
                self._setup_executed_init = True
            super().__init__(url, force_rollback=force_rollback, **options)
            if hasattr(url, "test_db_url"):
                self.test_db_url = url.test_db_url
            else:
                if test_prefix:
                    self.url = self.url.replace(database=f"{test_prefix}{self.url.database}")
                self.test_db_url = str(self.url)
        else:
            if lazy_setup is None:
                lazy_setup = self.testclient_default_lazy_setup
            if force_rollback is None:
                force_rollback = self.testclient_default_force_rollback
            if poll_interval is None:
                poll_interval = self.testclient_default_poll_interval
            self.use_existing = use_existing
            self.drop = drop_database
            super().__init__(
                url,
                force_rollback=force_rollback,
                full_isolation=full_isolation,
                poll_interval=poll_interval,
                **options,
            )
            if test_prefix:
                self.url = self.url.replace(database=f"{test_prefix}{self.url.database}")
            self.test_db_url = str(self.url)
            # if None or False
            if not lazy_setup:
                self.setup_protected(self.testclient_operation_timeout_init)
                self._setup_executed_init = True

    async def setup(self) -> None:
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

    def setup_protected(self, operation_timeout: float) -> None:
        thread = ThreadPassingExceptions(target=asyncio.run, args=[self.setup()])
        thread.start()
        with contextlib.suppress(TimeoutError):
            thread.join(operation_timeout)

    async def connect_hook(self) -> None:
        if not self._setup_executed_init:
            self.setup_protected(self.testclient_operation_timeout)
        await super().connect_hook()

    async def is_database_exist(self) -> Any:
        """
        Checks if a database exists.
        """
        return await self.database_exists(self.test_db_url)

    @classmethod
    async def database_exists(cls, url: Union[str, "sqlalchemy.URL", DatabaseURL]) -> bool:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect_name = url.sqla_url.get_dialect(True).name
        if dialect_name == "postgresql":
            text = "SELECT 1 FROM pg_database WHERE datname=:database"
            for db in (database, None, "postgres", "template1", "template0"):
                url = url.replace(database=db)
                try:
                    async with Database(
                        url, full_isolation=False, force_rollback=False
                    ) as db_client:
                        if await db_client.fetch_val(
                            # if we can connect to the db, it exists
                            "SELECT 1"
                            if db == database
                            else sqlalchemy.text(text).bindparams(database=database)
                        ):
                            return True
                except Exception:
                    pass
            return False

        elif dialect_name == "mysql":
            for db in (database, None, "root"):
                url = url.replace(database=db)
                try:
                    async with Database(
                        url, full_isolation=False, force_rollback=False
                    ) as db_client:
                        if await db_client.fetch_val(
                            (
                                # if we can connect to the db, it exists
                                "SELECT 1"
                                if db == database
                                else sqlalchemy.text(
                                    "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :database"
                                ).bindparams(database=database)
                            ),
                        ):
                            return True
                except Exception:
                    pass
            return False

        elif dialect_name == "sqlite":
            if database:
                return database == ":memory:" or os.path.exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            try:
                async with Database(url, full_isolation=False, force_rollback=False) as db_client:
                    await db_client.fetch_val("SELECT 1")
                    return True
            except Exception:
                return False

    @classmethod
    async def create_database(
        cls,
        url: Union[str, "sqlalchemy.URL", DatabaseURL],
        encoding: str = "utf8",
        template: Any = None,
    ) -> None:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect = url.sqla_url.get_dialect(True)
        dialect_name = dialect.name
        dialect_driver = dialect.driver

        # we don't want to connect to a not existing db
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
            db_client = Database(
                url,
                isolation_level="AUTOCOMMIT",
                force_rollback=False,
                full_isolation=False,
            )
        else:
            db_client = Database(url, force_rollback=False, full_isolation=False)
        async with db_client:
            if dialect_name == "postgresql":
                if not template:
                    template = "template1"

                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    text = (
                        f"CREATE DATABASE {quote(database)} ENCODING "
                        f"'{encoding}' TEMPLATE {quote(template)}"
                    )
                    await conn.execute(sqlalchemy.text(text))

            elif dialect_name == "mysql":
                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    text = f"CREATE DATABASE {quote(database)} CHARACTER SET = '{quote(encoding)}'"
                    await conn.execute(sqlalchemy.text(text))

            elif dialect_name == "sqlite" and database != ":memory:":
                if database:
                    # create a sqlite file
                    async with (
                        db_client.connection() as conn,
                        conn.transaction(force_rollback=False),
                    ):
                        await conn.execute("CREATE TABLE _dropme_DB(id int)")
                        await conn.execute("DROP TABLE _dropme_DB")

            else:
                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    await conn.execute(sqlalchemy.text(f"CREATE DATABASE {quote(database)}"))

    @classmethod
    async def drop_database(
        cls, url: Union[str, "sqlalchemy.URL", DatabaseURL], *, use_if_exists: bool = True
    ) -> None:
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        exists_text = "IF EXISTS " if use_if_exists else ""
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

        if dialect_name == "sqlite":
            if database and database != ":memory:":
                # WARNING: this can be used to remove arbitary files
                # make sure you sanitize the database name
                with contextlib.suppress(FileNotFoundError):
                    os.remove(database)
            return

        if (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
            dialect_name == "postgresql"
            and dialect_driver in {"asyncpg", "pg8000", "psycopg", "psycopg2", "psycopg2cffi"}
        ):
            db_client = Database(
                url,
                isolation_level="AUTOCOMMIT",
                force_rollback=False,
                full_isolation=False,
            )
        else:
            db_client = Database(url, force_rollback=False, full_isolation=False)
        async with db_client:
            if dialect_name.startswith("postgres"):
                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    # Disconnect all users from the database we are dropping.
                    server_version_raw = (
                        await conn.fetch_val(
                            "SELECT setting FROM pg_settings WHERE name = 'server_version'"
                        )
                    ).split(" ")[0]
                    version = tuple(map(int, server_version_raw.split(".")))
                    pid_column = "pid" if (version >= (9, 2)) else "procpid"
                    quoted_db = quote(database)
                    text = f"""
                    SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{quoted_db}'
                    AND {pid_column} <> pg_backend_pid();
                    """
                    await conn.execute(text)

                    # Drop the database.
                    text = f"DROP DATABASE {exists_text}{quoted_db}"
                    with contextlib.suppress(ProgrammingError):
                        await conn.execute(text)
            else:
                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    text = f"DROP DATABASE {exists_text}{quote(database)}"
                    with contextlib.suppress(ProgrammingError):
                        await conn.execute(text)

    def drop_db_protected(self) -> None:
        thread = ThreadPassingExceptions(
            target=asyncio.run, args=[self.drop_database(self.test_db_url)]
        )
        thread.start()
        with contextlib.suppress(TimeoutError):
            thread.join(self.testclient_operation_timeout)

    async def disconnect_hook(self) -> None:
        # next connect the setup routine is reexecuted
        self._setup_executed_init = False
        if self.drop:
            self.drop_db_protected()
        await super().disconnect_hook()
