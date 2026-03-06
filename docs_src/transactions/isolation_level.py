import asyncio

from databasez import Database


async def add_excitement(connection, id: int) -> None:
    await connection.execute(
        "UPDATE notes SET text = :text WHERE id = :id",
        {"id": id, "text": "databasez is cool!!!"},
    )


async def main() -> None:
    async with Database("sqlite+aiosqlite:///example.db") as database:
        await database.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )
        await database.execute("DELETE FROM notes")

        async with database.transaction(isolation_level="SERIALIZABLE"):
            await database.execute("INSERT INTO notes(id, text) values (1, 'databasez is cool')")
            await asyncio.create_task(add_excitement(database.connection(), id=1))

        value = await database.fetch_val("SELECT text FROM notes WHERE id = :id", {"id": 1})
        assert value == "databasez is cool!!!"
