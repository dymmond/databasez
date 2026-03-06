# Connecting to a database

Databasez handles database connection pooling and transaction management
with minimal fuss. It automatically deals with acquiring and releasing
connections and supports transaction APIs for both common and advanced flows.

## Database

This is the main object used for connections and queries.

```python
from databasez import Database
```

## Constructor parameters

- `url`: connection string, `DatabaseURL`, SQLAlchemy `URL`, another `Database`, or `None`.
- `force_rollback`: enables global rollback mode. Can be overwritten at runtime.
- `full_isolation`: runs the global rollback connection on a dedicated thread/loop.
- `poll_interval`: cross-loop poll interval for multiloop helpers.
- `config`: dictionary-style configuration alternative to `url`.
- `**options`: forwarded backend/engine options.

!!! Warning
    Use either `url` or `config`, not both.

### `config` shape

```python
"connection": {
    "credentials": {
        "scheme": "postgresql+asyncpg",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "password",
        "database": "my_db",
        "options": {
            "ssl": "true"
        }
    }
}
```

Example:

```python
{! ../docs_src/connections/as_dict.py!}
```

MSSQL-style example:

```python
{! ../docs_src/connections/mssql.py!}
```

## Main attributes and helpers

- `force_rollback`:
  Context-aware descriptor. Supports assignment/reset and context-manager usage.
- `engine`:
  Exposes SQLAlchemy `AsyncEngine` when connected.
- `asgi(...)`:
  ASGI lifespan wrapper for automatic connect/disconnect.

## Connecting and disconnecting

Use async context manager:

```python
async with Database(DATABASE_URL) as database:
    await database.fetch_val("SELECT 1")
```

Or explicit lifecycle calls:

```python
database = Database(DATABASE_URL)
await database.connect()
await database.fetch_val("SELECT 1")
await database.disconnect()
```

## Query shortcuts on `Database`

`Database` exposes connection-backed shortcuts:

- `fetch_all`
- `fetch_one`
- `fetch_val`
- `execute`
- `execute_many`
- `iterate`
- `batched_iterate`
- `run_sync`
- `create_all`
- `drop_all`

For detailed query examples see [Queries](./queries.md).

## Connection options

The SQLAlchemy backend extracts some URL query options automatically:

- `ssl`
- `echo`
- `echo_pool`
- `pool_size`
- `max_overflow`
- `pool_recycle`
- `isolation_level`

Example:

```python
database = Database("postgresql+asyncpg://localhost/example?ssl=true&pool_size=20")
```

Keyword options can also be passed directly:

```python
database = Database("postgresql+asyncpg://localhost/example", ssl=True, pool_size=20)
```

## Force rollback

Force rollback can be set globally or temporarily:

```python
database = Database("sqlite+aiosqlite:///example.db", force_rollback=True)

with database.force_rollback(False):
    # next connection() calls in this context are not forced to rollback
    pass
```

For a runnable example see:

```python
{!> ../docs_src/connections/force_rollback.py !}
```

## ASGI integration helper

`Database.asgi()` wraps an ASGI app and hooks startup/shutdown lifecycle events.

```python
{!> ../docs_src/integrations/django.py !}
```

## Reusing the SQLAlchemy engine

If a database is connected, you can access `database.engine` and use SQLAlchemy APIs directly.

## Debugging multiloop behavior

To debug blocked cross-loop waits, set:

`databasez.utils.DATABASEZ_RESULT_TIMEOUT = <seconds>`

This raises instead of waiting forever.

For deeper internals:

- [Core Concepts](./core-concepts.md)
- [Architecture Overview](./architecture-overview.md)
- [Troubleshooting](./troubleshooting.md)
