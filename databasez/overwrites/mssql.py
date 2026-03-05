from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    """Transaction backend for Microsoft SQL Server.

    Defaults to the ``READ UNCOMMITTED`` isolation level, which is
    commonly used with MSSQL to avoid locking issues during reads.
    """

    def get_default_transaction_isolation_level(self, is_root: bool, **extra_options: Any) -> str:
        """Return the default transaction isolation level.

        Args:
            is_root: Whether this is a root (non-nested) transaction.
            **extra_options: Additional transaction options.

        Returns:
            str: ``"READ UNCOMMITTED"``.
        """
        return "READ UNCOMMITTED"


class Database(SQLAlchemyDatabase):
    """Database backend for Microsoft SQL Server.

    Automatically swaps the synchronous ``pyodbc`` driver for the async
    ``aioodbc`` driver and injects JSON serializer / deserializer
    defaults.
    """

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise MSSQL-specific connection options.

        Replaces the ``pyodbc`` (or unset) driver with ``aioodbc`` and
        populates default JSON serialization helpers.

        Args:
            database_url: The original database URL.
            **options: Arbitrary connection options.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: The rewritten URL and
                remaining options dictionary.
        """
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pyodbc"}:
            database_url_new = database_url_new.replace(driver="aioodbc")
        options.setdefault("json_serializer", self.json_serializer)
        options.setdefault("json_deserializer", self.json_deserializer)
        return database_url_new, options
