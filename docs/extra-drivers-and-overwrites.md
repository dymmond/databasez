# Extra drivers and overwrites

## Extra drivers

Databasez registers two additional SQLAlchemy dialects:

- `jdbc`
- `dbapi2`

These are useful when there is no native async SQLAlchemy driver for your target.

!!! Warning
    Both drivers intentionally have limited behavior compared with first-class async dialects.

## JDBC

### URL format

```text
jdbc+<dsn-driver>://<dsn>?jdbc_driver=<java.class.Name>
```

Examples:

- `jdbc+sqlite://testsuite.sqlite3?classpath=tests/sqlite-jdbc-3.47.0.0.jar&jdbc_driver=org.sqlite.JDBC`
- `jdbc+postgresql://localhost:5432/app?...`

### JDBC options

- `jdbc_driver`: Java driver class name.
- `jdbc_driver_args`: JSON driver kwargs.
- `jdbc_dsn_driver`: explicit DSN driver override.
- `classpath`: jar path(s), passed as keyword argument or query option.

### JDBC direct keyword options

- `transform_reflected_names`: `"none" | "upper" | "lower"`.
- `use_code_datatype`: use JDBC integer type code for reflected datatype mapping.

!!! Warning
    Injecting additional classpaths into an already-running JVM is not always reliable. Prefer passing all required jars up front.

## DBAPI2

### URL format

```text
dbapi2://<dsn>
dbapi2+<dsn-driver>://<dsn>
```

### Required option

- `dbapi_path`: Python import path for the DBAPI module.

### DBAPI2 options

- `dbapi_driver_args`: JSON driver kwargs.
- `dbapi_dsn_driver`: DSN driver override.
- `dbapi_pool`: `"thread"` (default) or `"process"`.
- `dbapi_force_async_wrapper`: `True`, `False`, or `None` (auto-detect).

## Overwrites

Overwrites customize behavior per dialect by exposing classes in modules under `databasez.overwrites`.

Recognized class names:

- `Database`
- `Connection`
- `Transaction`

They must inherit from:

- `DatabaseBackend`
- `ConnectionBackend`
- `TransactionBackend`

In practice, inherit from `databasez.sqlalchemy` backends and override only what you need.

Built-in examples:

- `databasez.overwrites.postgresql`
- `databasez.overwrites.mysql`
- `databasez.overwrites.sqlite`
- `databasez.overwrites.mssql`
- `databasez.overwrites.jdbc`
- `databasez.overwrites.dbapi2`

## What built-in overwrites do

- set async driver defaults (`psycopg`, `asyncmy`, `aiosqlite`, `aioodbc`)
- set default transaction isolation level per dialect where needed
- adapt iteration behavior (e.g. PostgreSQL transactional streaming)
- move driver-specific options from kwargs into URL query parameters for custom dialects
