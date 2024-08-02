import typing

from databasez.sqlalchemy import SQLAlchemyConnection, SQLAlchemyDatabase, SQLAlchemyTransaction

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: typing.Any
    ) -> str:
        return "READ COMMITTED"


class Connection(SQLAlchemyConnection):
    async def execute(self, stmt: typing.Any) -> int:
        """
        Executes statement and returns the last row id (query) or the row count of updates.

        """
        with await self.execute_raw(stmt) as result:
            if result.is_insert and result.returned_defaults:
                return typing.cast(int, result.returned_defaults[0])
            return typing.cast(int, result.rowcount)


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Any,
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pyodbc"}:
            database_url_new = database_url_new.replace(driver="aioodbc")
        options.setdefault("json_serializer", self.json_serializer)
        options.setdefault("json_deserializer", self.json_deserializer)
        return database_url_new, options
