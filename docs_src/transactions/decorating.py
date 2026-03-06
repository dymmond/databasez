from databasez import Database

database = Database("sqlite+aiosqlite:///example.db")


@database.transaction()
async def create_note(text: str) -> None:
    await database.execute("INSERT INTO notes(text) VALUES (:text)", values={"text": text})


async def main() -> None:
    async with database:
        await database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )
        await create_note("created inside a transaction")
