import typing

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: typing.Any
    ) -> typing.Optional[str]:
        return None


class Database(SQLAlchemyDatabase):
    default_isolation_level = None

    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Any,
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        new_query_options = dict(database_url.options)
        if database_url_new.driver:
            new_query_options["dbapi2_dsn_driver"] = database_url_new.driver
        return database_url_new.replace(driver=None, options=new_query_options), options
