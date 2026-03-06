# Tests and Migrations

Databasez is designed for production services and test isolation flows.

## Test isolation

For integration tests, use [DatabaseTestClient](./test-client.md).

Two common strategies:

1. one reusable test database with rollback per test
2. create/drop test database around suite runs

Databasez supports both with `force_rollback`, `drop_database`, and `use_existing`.

## Migrations

Databasez works with SQLAlchemy Core metadata, so Alembic integration is straightforward.

```shell
$ pip install alembic
$ alembic init migrations
```

In `alembic.ini`, remove:

```shell
sqlalchemy.url = driver://user:pass@localhost/dbname
```

In `migrations/env.py`, set URL and metadata:

```python
from alembic import context

from myapp.settings import DATABASE_URL
from myapp.tables import metadata

config = context.config
config.set_main_option("sqlalchemy.url", str(DATABASE_URL))
target_metadata = metadata
```

## Sync driver note

Alembic migrations run with synchronous SQLAlchemy engines/drivers. This is normal and separate from your async runtime queries.

## Examples

[Edgy][edgy] is a good real-world example from the same ecosystem.

[edgy]: https://edgy.dymmond.com
