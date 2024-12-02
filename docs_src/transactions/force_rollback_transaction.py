from databasez import Database

database = Database("sqlite:///foo.sqlite")


def test_foo():
    async with database:
        await database.execute(...)

        async with database.transaction(force_rollback=True):
            # this is rolled back
            await database.execute(...)
            async with database.transaction(force_rollback=False):
                ...
                # this is saved
