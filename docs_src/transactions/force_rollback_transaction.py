from databasez import Database

database = Database("sqlite+aiosqlite:///example.db")


async def test_foo() -> None:
    async with database:
        await database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )
        await database.execute("DELETE FROM notes")

        async with database.transaction():
            await database.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "saved"})

            async with database.transaction(force_rollback=True):
                await database.execute(
                    "INSERT INTO notes(text) VALUES (:text)", {"text": "rolled"}
                )

        rows = await database.fetch_all("SELECT text FROM notes ORDER BY id")
        assert [row.text for row in rows] == ["saved"]
