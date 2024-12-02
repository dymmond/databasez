import contextlib

import databasez

async with databasez.Database("database_url") as db:
    async with db.transaction() as outer:
        # Do something in the outer transaction
        ...

        # Suppress to prevent influence on the outer transaction
        with contextlib.suppress(ValueError):
            async with db.transaction():
                # Do something in the inner transaction
                ...

                raise ValueError("Abort the inner transaction")

    # Observe the results of the outer transaction,
    # without effects from the inner transaction.
    await db.fetch_all("SELECT * FROM ...")
