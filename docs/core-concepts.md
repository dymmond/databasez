# Core Concepts

This page explains the concepts that appear across all Databasez APIs.

## Database

`Database` owns:

- backend selection (`databasez.overwrites.*`)
- engine/pool lifecycle (`connect`, `disconnect`)
- connection routing (`connection()`)
- high-level query shortcuts (`fetch_*`, `execute*`, `iterate*`)

It is safe to treat `Database` as the entry point for most applications.

## Task-local connections

Connections are stored per `asyncio.Task`. Inside one task, repeated `database.connection()` calls reuse the same `Connection`. Different tasks get different `Connection` objects.

Use this model when you need explicit connection scope:

```python
async with database.connection() as connection:
    await connection.execute(
        "INSERT INTO notes(text) VALUES (:text)",
        {"text": "example"},
    )
```

## Force rollback

`force_rollback` is context-aware and can be controlled in three ways:

- constructor: `Database(..., force_rollback=True)`
- descriptor assignment: `database.force_rollback = True` / `del database.force_rollback`
- temporary override: `with database.force_rollback(True): ...`

When active, Databasez uses one global connection wrapped in a rollback transaction so data changes are not persisted after context exit. This is mainly for tests.

## Transactions

Transactions are lazy and support:

1. context manager: `async with database.transaction(): ...`
2. decorator: `@database.transaction()`
3. manual control: `tx = await database.transaction(); await tx.commit()`

Nested transactions are implemented through savepoints when supported by the backend.

## Full isolation

`full_isolation=True` runs the global rollback connection in a dedicated background thread/loop. This is useful for test scenarios that combine force rollback and multi-threaded access.

## Timeouts

Most query methods accept `timeout=...`.

- method-level timeout: per call/per item.
- global cross-loop timeout: `databasez.utils.DATABASEZ_RESULT_TIMEOUT`.

Use the global timeout only for debugging lockups or deadlock-like behavior in multiloop/multithread setups.

## Next steps

- [Database](./database.md)
- [Connections & Transactions](./connections-and-transactions.md)
- [Troubleshooting](./troubleshooting.md)
