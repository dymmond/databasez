from databasez import Database

database = Database("<URL>")


@database.transaction()
async def create_users(request):
    ...
    # do something


async def main():
    async with database:
        # now the transaction is activated
        await create_users()
