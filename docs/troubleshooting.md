# Troubleshooting

## Deadlock-like behavior with `force_rollback`

When `force_rollback=True`, Databasez uses one global connection. If you open nested query flows on the same global connection (especially while iterating), you can block yourself.

Try one of these:

- run the extra query in another task/flow
- temporarily disable rollback in a narrow scope:
  `with database.force_rollback(False): ...`
- use `full_isolation=True` for thread-heavy test setups

## Multiloop timeouts or hangs

Set a global debug timeout:

```python
from databasez import utils

utils.DATABASEZ_RESULT_TIMEOUT = 5
```

This forces cross-loop waits to fail fast and gives you a traceback path.

## JDBC reflection surprises

Some JDBC drivers return transformed identifier names or limited metadata.

Try:

- `transform_reflected_names="lower"` or `"upper"`
- `use_code_datatype=True`
- explicit classpath list with every required driver jar

See [Extra drivers and overwrites](./extra-drivers-and-overwrites.md).

## SQLite behavior differences

- decimal handling can differ (`Decimal` may round depending on driver conversion)
- temporary tables and lock behavior can differ from PostgreSQL/MySQL
- `:memory:` databases are process-local

## Driver defaults and URL confusion

Databasez may auto-upgrade default drivers:

- PostgreSQL: to `psycopg`
- MySQL: to `asyncmy`
- MSSQL: to `aioodbc`
- SQLite: to `aiosqlite`

If behavior is unexpected, print `database.url` after initialization and verify the final URL.

## Test database cleanup

`DatabaseTestClient(drop_database=True)` tries to drop at disconnect. If setup/drop fails due permissions or dialect constraints, drop can be disabled internally for safety.

Check:

- server permissions
- active sessions still connected
- dialect admin database rules (`postgres`, `master`, `defaultdb`)
