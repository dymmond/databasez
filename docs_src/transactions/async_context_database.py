from databasez import Database


async def main() -> None:
    async with Database("sqlite+aiosqlite:///example.db") as database:
        await database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )

        async with database.transaction():
            await database.execute(
                "INSERT INTO notes(text) VALUES (:text)", values={"text": "committed"}
            )

        async with database.transaction(force_rollback=True):
            await database.execute(
                "INSERT INTO notes(text) VALUES (:text)", {"text": "rolled back"}
            )
