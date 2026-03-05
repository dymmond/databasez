from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

# ensure jpype.dbapi2 is initialized. Prevent race condition.
import jpype.dbapi2  # noqa
import jpype.imports  # noqa
from jpype import addClassPath, isJVMStarted, startJVM

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


seen_classpathes: set[str] = set()
"""Module-level set tracking classpath entries already added to the JVM.

Prevents duplicate classpath additions across multiple ``Database.connect``
calls within the same process.
"""


class Transaction(SQLAlchemyTransaction):
    """Transaction backend for JDBC drivers.

    Disables default transaction isolation level management, deferring
    control entirely to the underlying JDBC driver.
    """

    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        """Return the default transaction isolation level.

        Always returns ``None`` so that the JDBC driver manages its own
        isolation level.

        Args:
            is_root: Whether this is a root (non-nested) transaction.
            **extra_options: Additional transaction options.

        Returns:
            str | None: Always ``None``.
        """
        return None


class Database(SQLAlchemyDatabase):
    """Database backend for JDBC drivers via JPype.

    Extends the default SQLAlchemy database backend with JDBC-specific
    option extraction and JVM lifecycle management.  On first connect the
    JVM is started (if not already running) and requested classpath
    entries are registered.

    Attributes:
        default_isolation_level: Set to ``None`` to bypass isolation
            level configuration.
    """

    default_isolation_level = None

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise JDBC-specific connection options.

        Moves the JDBC DSN driver name, classpath entries, and driver
        arguments from ``options`` into the URL query string so the JDBC
        dialect can consume them at connect time.

        Args:
            database_url: The original database URL.
            **options: Arbitrary connection options that may contain
                ``classpath`` and ``jdbc_driver_args``.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: A 2-tuple of the
                rewritten ``DatabaseURL`` (with driver set to ``None``
                and enriched query options) and the remaining options
                dictionary.
        """
        database_url_new, options = super().extract_options(database_url, **options)
        new_query_options = dict(database_url.options)
        if database_url_new.driver:
            new_query_options["jdbc_dsn_driver"] = database_url_new.driver
        if "classpath" in new_query_options:
            old_classpath = options.pop("classpath", None)
            new_classpath: list[str] = []
            if old_classpath:
                if isinstance(old_classpath, str):
                    new_classpath.append(old_classpath)
                else:
                    new_classpath.extend(old_classpath)
            query_classpath = new_query_options.pop("classpath")
            if query_classpath:
                if isinstance(query_classpath, str):
                    new_classpath.append(query_classpath)
                else:
                    new_classpath.extend(query_classpath)
            options["classpath"] = new_classpath
        if "jdbc_driver_args" in options:
            jdbc_driver_args = options.pop("jdbc_driver_args")
            if isinstance(jdbc_driver_args, str):
                new_query_options["jdbc_driver_args"] = jdbc_driver_args
            else:
                new_query_options["jdbc_driver_args"] = self.json_serializer(jdbc_driver_args)
        return database_url_new.replace(driver=None, options=new_query_options), options

    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        """Open a JDBC database connection.

        Registers any requested Java classpath entries, starts the JVM
        if it is not already running, and delegates to the parent
        ``connect`` implementation.

        Args:
            database_url: The database URL to connect to.
            **options: Connection options.  The ``classpath`` key, if
                present, is consumed here and not forwarded.

        Raises:
            Exception: If a classpath entry cannot be added.
        """
        classpath: str | list[str] | None = options.pop("classpath", None)
        if classpath:
            if isinstance(classpath, str):
                classpath = [classpath]
            parent_dir = Path(os.getcwd())
            for clpath in classpath:
                _classpath: str = str(clpath)
                if _classpath not in seen_classpathes:
                    seen_classpathes.add(_classpath)
                    # add original
                    try:
                        addClassPath(parent_dir.joinpath(clpath))
                    except Exception as exc:
                        raise Exception(str(exc)) from None

        if not isJVMStarted():
            startJVM()
        await super().connect(database_url, **options)
