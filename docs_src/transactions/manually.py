from databasez import Database


async def main():
    async with Database("<URL>") as database, database.connection() as connection:
        # get the activated transaction
        transaction = await connection.transaction()

        try:
            # do something
            ...
        except Exception:
            await transaction.rollback()
        else:
            await transaction.commit()
