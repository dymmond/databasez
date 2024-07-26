import typing

from databasez.sqlalchemy import SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    pass
