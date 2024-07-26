import typing

from jpype import isStarted, startJVM

from databasez.sqlalchemy import SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        if not isStarted():
            startJVM()
        await super().connect(database_url, **options)
