import contextlib

import databasez


async def main() -> None:
    async with databasez.Database("sqlite+aiosqlite:///example.db") as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text VARCHAR(100))"
        )
        await db.execute("DELETE FROM notes")

        async with db.transaction():
            await db.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "outer"})

            # Suppress so the inner transaction rolls back without breaking
            # the outer one.
            with contextlib.suppress(ValueError):
                async with db.transaction():
                    await db.execute("INSERT INTO notes(text) VALUES (:text)", {"text": "inner"})
                    raise ValueError("Abort the inner transaction")

        rows = await db.fetch_all("SELECT text FROM notes ORDER BY id")
        assert [row.text for row in rows] == ["outer"]
