from databasez import Database


async def main():
    async with Database("<URL>") as database, database.connection() as connection:
        # do something
        async with connection.transaction():
            ...

        # check something and then reset
        async with connection.transaction(force_rollback=True):
            ...
