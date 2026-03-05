# Test Client

When testing an application that uses a database, you often need a dedicated test database
to avoid polluting your development data. ``DatabaseTestClient`` automates test database
creation, connection, and teardown.

Before continuing, make sure you have the databasez test client installed with the needed
requirements.

```
$ pip install databasez[testing]
```

## History

Many frameworks and helpers exist for managing test databases. Django, for example,
creates a test database automatically when running unit tests.

When ``DatabaseTestClient`` was first created, ``sqlalchemy_utils`` was the package used
for database creation and dropping. However, up to version ``0.40.0`` that package lacked
async support. ``DatabaseTestClient`` was built to provide async-native database lifecycle
management directly within **databasez**.

## DatabaseTestClient

This is the client you have been waiting for. This object does a lot of magic for you and will
help you manage those stubborn tests that should land on a `test_` database.

```python
from databasez.testclient import DatabaseTestClient
```

### Parameters

* **url** - The database url for your database.
            It supports the same types like normal Database objects and has a special handling for subclasses of DatabaseTestClient.

* **force_rollback** - This will ensure that all database connections are run within a transaction
                       that rollbacks once the database is disconnected.

    <sup>Default: `None`, copy default or `testclient_default_force_rollback` (defaults to `False`) </sup>

* **full_isolation** - Special mode for using force_rollback with nested queries. This parameter fully isolates the global connection
                       in an extra thread. This way it is possible to use blocking operations like locks with force_rollback.
                       This parameter has no use when used without force_rollback and causes a slightly slower setup (Lock is initialized).
                       It is required for edgy or other frameworks which use threads in tests and the force_rollback parameter.
                       For the DatabaseTestClient it is enabled by default.

    <sup>Default: `None`, copy default or `testclient_default_full_isolation` (defaults to `True`) </sup>

* **poll_interval** - When using multithreading, the poll_interval is used to retrieve results from other loops. It defaults to a sane value.

    <sup>Default: `None`, copy default or `testclient_default_poll_interval` </sup>

* **lazy_setup** - This sets up the db first up on connect not in init.

    <sup>Default: `None`, True if copying a database or `testclient_default_lazy_setup` (defaults to `False`)</sup>

* **use_existing** - Uses the existing `test_` database if previously created and not dropped.

    <sup>Default: `testclient_default_use_existing` (defaults to `False`)</sup>

* **drop_database** - Ensures that after the tests, the database is dropped. The corresponding attribute is `drop`.
                      When the setup fails, it is automatically set to `False`.

    <sup>Default: `testclient_default_drop_database` (defaults to `False`)</sup>

* **test_prefix** - Allow a custom test prefix or leave empty to use the url instead without changes.

    <sup>Default: `testclient_default_test_prefix` (defaults to `test_`)</sup>

### Subclassing

The defaults of all parameters except the url can be changed by providing in a subclass a different value for the attribute:

`testclient_default_<parameter name>`

There are also 2 knobs for the operation timeout (setting up DB, dropping databases):

`testclient_operation_timeout`

Default: `4`.

and the limit

`testclient_operation_timeout_init` for the non-lazy setup in init of the database.

Default: `8`.

### How to use it

This is the easiest part because is already very familiar with the `Database` used by Edgy or Saffier. In
fact, this is an extension of that same object with a lot of testing flavours.

Let us assume you have a database url like this following:

```shell
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/my_db"
```

We know the database is called `my_db`, right?

When using the `DatabaseTestClient`, the client will ensure the tests will land on a `test_my_db`.

Pretty cool, right?

Nothing like an example to see it in action.

```python title="tests.py"
{!> ../docs_src/testclient/tests.py !}
```

#### What is happening

Well, this is rather complex test and actually a real one from Saffier and what you can see is
that is using the `DatabaseTestClient` which means the tests against models, fields or whatever
database operation you want will be on a `test_` database.

But you can see a `drop_database=True`, so what is that?

Well `drop_database=True` means that by the end of the tests finish running, drops the database
into oblivion.
