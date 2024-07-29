import typing

from databasez.sqlalchemy import SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pscopg2"}:
            database_url_new = database_url_new.replace(driver="psycopg")
        return database_url_new, options
