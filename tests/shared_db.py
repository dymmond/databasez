import datetime
import typing
from unittest.mock import MagicMock

import sqlalchemy

from databasez import Database
from databasez.testclient import DatabaseTestClient


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MyEpochType(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.Integer

    epoch = datetime.date(1970, 1, 1)

    def process_bind_param(self, value, dialect):
        return (value - self.epoch).days

    def process_result_value(self, value, dialect):
        return self.epoch + datetime.timedelta(days=value)


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)

# Used to test DateTime
articles = sqlalchemy.Table(
    "articles",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", sqlalchemy.DateTime),
)

# Used to test JSON
session = sqlalchemy.Table(
    "session",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("data", sqlalchemy.JSON),
)

# Used to test custom column types
custom_date = sqlalchemy.Table(
    "custom_date",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", MyEpochType),
)

# Used to test Numeric
prices = sqlalchemy.Table(
    "prices",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("price", sqlalchemy.Numeric(precision=30, scale=20)),
)


async def database_client(url: typing.Union[dict, str]) -> DatabaseTestClient:
    if isinstance(url, str):
        is_sqlite = url.startswith("sqlite")
        database = DatabaseTestClient(
            url, test_prefix="", use_existing=not is_sqlite, drop_database=is_sqlite
        )
    else:
        database = Database(config=url)
    await database.connect()
    await database.create_all(metadata)
    return database


async def stop_database_client(database: Database):
    await database.drop_all(metadata)
    await database.disconnect()
