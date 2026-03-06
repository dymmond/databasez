from databasez import Database

setup_database = Database("sqlite+aiosqlite:///testsuite.sqlite3")
database = Database("sqlite+aiosqlite:///testsuite.sqlite3", force_rollback=True)


async def test_foo() -> None:
    async with setup_database:
        await setup_database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )

    async with database:
        await database.execute("DELETE FROM notes")
        await database.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "inside-test"})
        row = await database.fetch_val("SELECT COUNT(*) FROM notes")
        assert row == 1

    async with database:
        row = await database.fetch_val("SELECT COUNT(*) FROM notes")
        assert row == 0
