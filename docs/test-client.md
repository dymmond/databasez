# Test Client

When testing applications that use databases, you usually want a dedicated test database so development data is never touched.

`DatabaseTestClient` automates:

- test database creation
- connection lifecycle
- optional cleanup/drop
- optional rollback isolation workflows

```python
from databasez.testclient import DatabaseTestClient
```

## Why it exists

Historically, `sqlalchemy_utils` covered some of this lifecycle, but async support and cross-dialect behavior are not always enough for complex async test suites.

`DatabaseTestClient` keeps this behavior in Databasez itself.

## Constructor parameters

- `url`: same accepted types as `Database`.
- `force_rollback`: rollback mode default for test sessions.
- `full_isolation`: dedicated thread/loop mode (enabled by default for test client).
- `poll_interval`: cross-loop polling interval.
- `use_existing`: reuse existing prefixed test database.
- `drop_database`: drop the test database on disconnect.
- `lazy_setup`: defer setup to first `connect`.
- `test_prefix`: prefix for generated test database names (default `test_`).

## Defaults you can override via subclassing

Class-level knobs:

- `testclient_default_full_isolation`
- `testclient_default_force_rollback`
- `testclient_default_poll_interval`
- `testclient_default_lazy_setup`
- `testclient_default_use_existing`
- `testclient_default_drop_database`
- `testclient_default_test_prefix`

Timeout knobs:

- `testclient_operation_timeout`
- `testclient_operation_timeout_init`

## Basic usage

```python
{!> ../docs_src/testclient/tests.py !}
```

## Behavior notes

- PostgreSQL/MSSQL/Cockroach use admin database redirection for create/drop operations.
- Some dialect/driver pairs use `AUTOCOMMIT` for admin DDL operations.
- On setup/drop errors, drop behavior may be disabled internally for safety.

## Related pages

- [Tests and Migrations](./tests-and-migrations.md)
- [Connections & Transactions](./connections-and-transactions.md)
- [Troubleshooting](./troubleshooting.md)
