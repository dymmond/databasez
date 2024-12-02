import asyncio

from databasez import Database, core


async def add_excitement(connection: core.Connection, id: int):
    await connection.execute(
        "UPDATE notes SET text = CONCAT(text, '!!!') WHERE id = :id", {"id": id}
    )


async with Database("database_url") as database:
    async with database.transaction():
        # This note won't exist until the transaction closes...
        await database.execute("INSERT INTO notes(id, text) values (1, 'databases is cool')")
        # ...but child tasks can use this connection now!
        await asyncio.create_task(add_excitement(database.connection(), id=1))

    async with database.transaction(isolation_level="serializable"):
        await database.fetch_val("SELECT text FROM notes WHERE id=1")
        # ^ returns: "databases is cool!!!"
