import asyncio
import contextlib
import os
import re
from typing import Any, Union

import sqlalchemy
from sqlalchemy.exc import OperationalError, ProgrammingError

from databasez import Database, DatabaseURL
from databasez.utils import DATABASEZ_POLL_INTERVAL, ThreadPassingExceptions, get_quoter


class DatabaseTestClient(Database):
    """Database client used only for testing.

    On initialisation (or first :meth:`connect`), creates a test database whose
    name is the original database name prefixed by *test_prefix* (default
    ``"test_"``).  The test database is optionally dropped on disconnect.

    Attributes:
        testclient_operation_timeout: Timeout in seconds for setup / teardown
            operations after the initial connect.
        testclient_operation_timeout_init: Timeout in seconds for the very
            first database creation (may be higher for cold starts).
        test_db_url: The resolved test database URL.
        testclient_default_full_isolation: Default for *full_isolation*.
        testclient_default_force_rollback: Default for *force_rollback*.
        testclient_default_poll_interval: Default poll interval.
        testclient_default_lazy_setup: Default for *lazy_setup*.
        testclient_default_use_existing: Default for *use_existing*.
        testclient_default_drop_database: Default for *drop_database*.
        testclient_default_test_prefix: Default name prefix.
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
        url: str | DatabaseURL | sqlalchemy.URL | Database | None = None,
        *,
        force_rollback: bool | None = None,
        full_isolation: bool | None = None,
        poll_interval: float | None = None,
        use_existing: bool | None = None,
        drop_database: bool | None = None,
        lazy_setup: bool | None = None,
        test_prefix: str | None = None,
        **options: Any,
    ) -> None:
        """Initialise the test client.

        Args:
            url: Connection URL, :class:`DatabaseURL`, SQLAlchemy URL, or an
                existing :class:`Database` to derive settings from.
            force_rollback: Whether to wrap every connection in a
                rolled-back transaction (default class attribute).
            full_isolation: Whether to create per-task connections.
            poll_interval: Timeout poll interval for multi-loop helpers.
            use_existing: If ``True``, reuse an existing test database
                rather than dropping and recreating.
            drop_database: If ``True``, drop the test database on
                disconnect.
            lazy_setup: If ``True`` (or ``None`` for non-Database URLs),
                defer database creation until the first :meth:`connect`.
            test_prefix: Prefix for the test database name.
            **options: Additional keyword arguments forwarded to
                :class:`~databasez.Database`.
        """
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
        """Create the test database if it does not already exist.

        When *use_existing* is ``False``, any existing test database is
        dropped first and a fresh one is created.  When ``True``, the
        database is created only if missing.
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
        """Run :meth:`setup` synchronously in a background thread.

        Uses :class:`~databasez.utils.ThreadPassingExceptions` so that
        exceptions propagate back to the caller.

        Args:
            operation_timeout: Maximum seconds to wait for setup.
        """
        thread = ThreadPassingExceptions(target=asyncio.run, args=[self.setup()])
        thread.start()
        with contextlib.suppress(TimeoutError):
            thread.join(operation_timeout)

    async def connect_hook(self) -> None:
        """Pre-connection hook: run deferred setup if necessary."""
        if not self._setup_executed_init:
            self.setup_protected(self.testclient_operation_timeout)
        await super().connect_hook()

    async def is_database_exist(self) -> Any:
        """Check whether the test database exists.

        Returns:
            bool: ``True`` when the test database exists.
        """
        return await self.database_exists(self.test_db_url)

    @classmethod
    async def database_exists(cls, url: Union[str, "sqlalchemy.URL", DatabaseURL]) -> bool:
        """Check whether a database exists at the given URL.

        Supports PostgreSQL, MySQL, SQLite, and generic fallback.

        Args:
            url: Database URL to check.

        Returns:
            bool: ``True`` if the database exists.
        """
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
    def _resolve_admin_url(
        cls,
        url: Union[str, "sqlalchemy.URL", DatabaseURL],
    ) -> tuple[DatabaseURL, str, str, str]:
        """Parse the URL and resolve the admin connection URL for DDL.

        Returns a tuple of (admin_url, database_name, dialect_name,
        dialect_driver) where *admin_url* has been redirected to the
        appropriate admin database for the dialect.

        Args:
            url: The original database URL.

        Returns:
            tuple: ``(admin_url, database_name, dialect_name, dialect_driver)``.
        """
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        database = url.database
        dialect = url.sqla_url.get_dialect(True)
        dialect_name: str = dialect.name
        dialect_driver: str = dialect.driver

        if dialect_name == "postgresql":
            url = url.replace(database="postgres")
        elif dialect_name == "mssql":
            url = url.replace(database="master")
        elif dialect_name == "cockroachdb":
            url = url.replace(database="defaultdb")
        elif dialect_name != "sqlite":
            url = url.replace(database=None)

        return url, database, dialect_name, dialect_driver

    @classmethod
    def _needs_autocommit(cls, dialect_name: str, dialect_driver: str) -> bool:
        """Return whether the dialect/driver pair requires AUTOCOMMIT for DDL.

        Args:
            dialect_name: The SQLAlchemy dialect name.
            dialect_driver: The SQLAlchemy driver name.

        Returns:
            bool: ``True`` if AUTOCOMMIT isolation is needed.
        """
        return (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
            dialect_name == "postgresql"
            and dialect_driver in {"asyncpg", "pg8000", "psycopg", "psycopg2", "psycopg2cffi"}
        )

    @classmethod
    def _admin_client(cls, url: DatabaseURL, dialect_name: str, dialect_driver: str) -> Database:
        """Create a :class:`Database` client suitable for DDL operations.

        Args:
            url: The admin database URL.
            dialect_name: The SQLAlchemy dialect name.
            dialect_driver: The SQLAlchemy driver name.

        Returns:
            Database: A database client configured for DDL.
        """
        if cls._needs_autocommit(dialect_name, dialect_driver):
            return Database(
                url,
                isolation_level="AUTOCOMMIT",
                force_rollback=False,
                full_isolation=False,
            )
        return Database(url, force_rollback=False, full_isolation=False)

    @staticmethod
    def _sanitize_charset_name(value: str) -> str:
        """Validate and normalize a charset/encoding identifier for DDL."""
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise ValueError(f"Invalid encoding name: {value!r}")
        return value

    @classmethod
    async def create_database(
        cls,
        url: Union[str, "sqlalchemy.URL", DatabaseURL],
        encoding: str = "utf8",
        template: str | None = None,
    ) -> None:
        """Create a new database.

        Supports PostgreSQL (with optional *template* and *encoding*),
        MySQL, SQLite, MSSQL, CockroachDB, and generic dialects.

        Args:
            url: Database URL.
            encoding: Character encoding (PostgreSQL / MySQL).
            template: PostgreSQL template name.
        """
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        url, database, dialect_name, dialect_driver = cls._resolve_admin_url(url)

        db_client = cls._admin_client(url, dialect_name, dialect_driver)
        async with db_client:
            if dialect_name == "postgresql":
                if not template:
                    template = "template1"
                encoding = cls._sanitize_charset_name(encoding)

                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    text = (
                        f"CREATE DATABASE {quote(database)} ENCODING "
                        f"'{encoding}' TEMPLATE {quote(template)}"
                    )
                    await conn.execute(sqlalchemy.text(text))

            elif dialect_name == "mysql":
                encoding = cls._sanitize_charset_name(encoding)
                async with db_client.connection() as conn:
                    quote = get_quoter(conn.async_connection)
                    text = f"CREATE DATABASE {quote(database)} CHARACTER SET {encoding}"
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
        """Drop a database.

        For PostgreSQL, terminates active connections before dropping.
        For SQLite, removes the database file.

        Args:
            url: Database URL.
            use_if_exists: Emit ``IF EXISTS`` in drop statement.
        """
        url = url if isinstance(url, DatabaseURL) else DatabaseURL(url)
        exists_text = "IF EXISTS " if use_if_exists else ""
        url, database, dialect_name, dialect_driver = cls._resolve_admin_url(url)

        if dialect_name == "sqlite":
            if database and database != ":memory:":
                # WARNING: this can be used to remove arbitary files
                # make sure you sanitize the database name
                with contextlib.suppress(FileNotFoundError):
                    os.remove(database)
            return

        db_client = cls._admin_client(url, dialect_name, dialect_driver)
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
                    text = sqlalchemy.text(
                        f"""
                    SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = :database
                    AND {pid_column} <> pg_backend_pid();
                    """
                    )
                    await conn.execute(text, {"database": database})

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
        """Drop the test database synchronously in a background thread."""
        thread = ThreadPassingExceptions(
            target=asyncio.run, args=[self.drop_database(self.test_db_url)]
        )
        thread.start()
        with contextlib.suppress(TimeoutError):
            thread.join(self.testclient_operation_timeout)

    async def disconnect_hook(self) -> None:
        """Post-disconnect hook: optionally drop the test database."""
        # next connect the setup routine is reexecuted
        self._setup_executed_init = False
        if self.drop:
            self.drop_db_protected()
        await super().disconnect_hook()
