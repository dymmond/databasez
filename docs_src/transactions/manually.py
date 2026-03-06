from databasez import Database


async def main() -> None:
    async with Database("sqlite+aiosqlite:///example.db") as database:
        await database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )
        transaction = await database.transaction()

        try:
            await database.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "manual"})
        except Exception:
            await transaction.rollback()
        else:
            await transaction.commit()
