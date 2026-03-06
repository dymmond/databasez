from databasez import Database


async def main() -> None:
    async with Database("sqlite+aiosqlite:///example.db") as database, database.connection() as connection:
        await connection.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )

        async with connection.transaction():
            await connection.execute(
                "INSERT INTO notes(text) VALUES (:text)", values={"text": "committed"}
            )

        async with connection.transaction(force_rollback=True):
            await connection.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "rolled back"})
