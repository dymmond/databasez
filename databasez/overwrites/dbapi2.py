from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    """Transaction backend for DBAPI2-compatible drivers.

    Disables default transaction isolation level management, leaving
    isolation control entirely to the underlying DBAPI2 driver.
    """

    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        """Return the default transaction isolation level.

        Always returns ``None`` to delegate isolation level management
        to the underlying DBAPI2 driver.

        Args:
            is_root: Whether this is a root (non-nested) transaction.
            **extra_options: Additional transaction options.

        Returns:
            str | None: Always ``None``.
        """
        return None


class Database(SQLAlchemyDatabase):
    """Database backend for DBAPI2-compatible drivers.

    Extends the default SQLAlchemy database backend with DBAPI2-specific
    option extraction logic.  Translates high-level connection options
    (``dbapi_pool``, ``dbapi_force_async_wrapper``, ``dbapi_driver_args``)
    into URL query parameters consumed by the DBAPI2 dialect.

    Attributes:
        default_isolation_level: Set to ``None`` to skip isolation level
            configuration.
    """

    default_isolation_level = None

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise DBAPI2-specific connection options.

        Moves driver-specific keys from ``options`` into the URL query
        string so that the DBAPI2 dialect can consume them at connect
        time.

        Args:
            database_url: The original database URL.
            **options: Arbitrary connection options that may contain
                ``dbapi_pool``, ``dbapi_force_async_wrapper``, and
                ``dbapi_driver_args``.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: A 2-tuple of the
                rewritten ``DatabaseURL`` (with driver set to ``None``
                and enriched query options) and the remaining options
                dictionary.
        """
        database_url_new, options = super().extract_options(database_url, **options)
        new_query_options = dict(database_url.options)
        if database_url_new.driver:
            new_query_options["dbapi_dsn_driver"] = database_url_new.driver
        if "dbapi_pool" in options:
            new_query_options["dbapi_pool"] = options.pop("dbapi_pool")
        if "dbapi_force_async_wrapper" in options:
            dbapi_force_async_wrapper = options.pop("dbapi_force_async_wrapper")
            if isinstance(dbapi_force_async_wrapper, bool):
                new_query_options["dbapi_force_async_wrapper"] = (
                    "true" if dbapi_force_async_wrapper else "false"
                )
            elif dbapi_force_async_wrapper is not None:
                new_query_options["dbapi_force_async_wrapper"] = dbapi_force_async_wrapper
        if "dbapi_driver_args" in options:
            dbapi_driver_args = options.pop("dbapi_driver_args")
            if isinstance(dbapi_driver_args, str):
                new_query_options["dbapi_driver_args"] = dbapi_driver_args
            else:
                new_query_options["dbapi_driver_args"] = self.json_serializer(dbapi_driver_args)
        return database_url_new.replace(driver=None, options=new_query_options), options
