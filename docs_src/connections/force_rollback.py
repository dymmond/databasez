from databasez import Database

database = Database("sqlite:///foo.sqlite", force_rollback=True)


def test_foo():
    async with database:
        ...
        # do the tests
    # and now everything is rolled back

    async with database:
        ...
        # do the tests
    # and now everything is rolled back again
