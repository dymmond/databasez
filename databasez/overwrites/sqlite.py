import typing

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(self, is_root: bool, **extra_options):
        return "READ UNCOMMITTED"


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver is None:
            database_url_new = database_url_new.replace(driver="aiosqlite")
        return database_url_new, options
