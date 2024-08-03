# Tests and Migrations

Databasez is designed to allow you to fully integrate with production
ready services, with API support for test isolation, and integration
with [Alembic][alembic] for database migrations.

## Test isolation

Databasez provides the [DatabaseTestClient](./test-client.md#databasetestclient) already configured
to create the `test_` database from you.

## Migrations

Because `databasez` uses SQLAlchemy core, you can integrate with [Alembic][alembic]
for database migration support. The same as you could do with `databases`.

```shell
$ pip install alembic
$ alembic init migrations
```

You'll want to set things up so that Alembic references the configured
`DATABASE_URL`, and uses your table metadata.

In `alembic.ini` remove the following line:

```shell
sqlalchemy.url = driver://user:pass@localhost/dbname
```

In `migrations/env.py`, you need to set the ``'sqlalchemy.url'`` configuration key,
and the `target_metadata` variable. You'll want something like this:

```python
# The Alembic Config object.
config = context.config

# Configure Alembic to use our DATABASE_URL and our table definitions.
# These are just examples - the exact setup will depend on whatever
# framework you're integrating against.
from myapp.settings import DATABASE_URL
from myapp.tables import metadata

config.set_main_option('sqlalchemy.url', str(DATABASE_URL))
target_metadata = metadata

...
```

Note that migrations will use a standard synchronous database driver,
rather than using the async drivers that `databases` provides support for.

This will also be the case if you're using SQLAlchemy's standard tooling, such
as using `metadata.create_all(engine)` to setup the database tables.

## Examples

[Edgy][edgy] (from the same author) is a good example as it has an internal migration system
based on Alembic and integrates with **Databasez**.

[alembic]: https://alembic.sqlalchemy.org/en/latest/
[edgy]: https://edgy.dymmon
