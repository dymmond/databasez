import pytest
import sqlalchemy

from databasez.testclient import DatabaseTestClient

DATABASE_URL = "sqlite+aiosqlite:///testsuite.sqlite3"

database = DatabaseTestClient(
    DATABASE_URL,
    test_prefix="test_",
    drop_database=True,
)

metadata = sqlalchemy.MetaData()
notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module", autouse=True)
async def create_test_tables():
    async with database:
        await database.create_all(metadata)
    yield
    async with database:
        await database.drop_all(metadata)


@pytest.fixture(autouse=True)
async def rollback_each_test():
    with database.force_rollback():
        async with database:
            yield


async def test_insert_and_read_note():
    await database.execute(notes.insert().values(text="example", completed=True))
    row = await database.fetch_one(notes.select())
    assert row.text == "example"
    assert row.completed is True
