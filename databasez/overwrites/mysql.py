from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    """Transaction backend for MySQL.

    Defaults to the ``READ COMMITTED`` isolation level, the most common
    choice for MySQL transactional workloads.
    """

    def get_default_transaction_isolation_level(self, is_root: bool, **extra_options: Any) -> str:
        """Return the default transaction isolation level.

        Args:
            is_root: Whether this is a root (non-nested) transaction.
            **extra_options: Additional transaction options.

        Returns:
            str: ``"READ COMMITTED"``.
        """
        return "READ COMMITTED"


class Database(SQLAlchemyDatabase):
    """Database backend for MySQL.

    Automatically swaps the synchronous ``pymysql`` driver for the async
    ``asyncmy`` driver and injects JSON serializer / deserializer
    defaults.
    """

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise MySQL-specific connection options.

        Replaces the ``pymysql`` (or unset) driver with ``asyncmy`` and
        populates default JSON serialization helpers.

        Args:
            database_url: The original database URL.
            **options: Arbitrary connection options.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: The rewritten URL and
                remaining options dictionary.
        """
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pymysql"}:
            database_url_new = database_url_new.replace(driver="asyncmy")
        options.setdefault("json_serializer", self.json_serializer)
        options.setdefault("json_deserializer", self.json_deserializer)
        return database_url_new, options
