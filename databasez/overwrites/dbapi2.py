from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        return None


class Database(SQLAlchemyDatabase):
    default_isolation_level = None

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
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
