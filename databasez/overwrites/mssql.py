from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(self, is_root: bool, **extra_options: Any) -> str:
        return "READ UNCOMMITTED"


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pyodbc"}:
            database_url_new = database_url_new.replace(driver="aioodbc")
        options.setdefault("json_serializer", self.json_serializer)
        options.setdefault("json_deserializer", self.json_deserializer)
        return database_url_new, options
