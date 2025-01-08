from databasez import Database


async def main():
    async with Database("<URL>") as database:
        # do something
        async with database.transaction():
            ...

        # check something and then reset
        async with database.transaction(force_rollback=True):
            ...
