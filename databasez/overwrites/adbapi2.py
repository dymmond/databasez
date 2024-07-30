import typing

from databasez.sqlalchemy import SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    pass


class Database(SQLAlchemyDatabase):
    pass
