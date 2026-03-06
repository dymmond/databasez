# Database URL

`DatabaseURL` is the URL utility used internally by `Database`, and can also be used directly.

```python
from databasez import DatabaseURL
```

## What is it

`DatabaseURL` parses, normalizes, and rewrites connection URLs.

It supports:

- `str`
- another `DatabaseURL`
- SQLAlchemy `URL`

## Why use it

It helps when you need to:

- inspect `dialect`, `driver`, `hostname`, `database`
- merge query options safely
- swap drivers (`pymysql` -> `asyncmy`, etc.)
- create test URLs (`test_` prefix)

## Common operations

### Parse and inspect

```python
from databasez import DatabaseURL

url = DatabaseURL("postgresql+asyncpg://user:pass@localhost:5432/app")

assert url.dialect == "postgresql"
assert url.driver == "asyncpg"
assert url.database == "app"
```

### Replace parts

```python
from databasez import DatabaseURL

url = DatabaseURL("sqlite+aiosqlite:///example.db")
test_url = url.replace(database="test_example.db")
```

### Work with options

```python
from databasez import DatabaseURL

url = DatabaseURL("postgresql://localhost/app?ssl=true&pool_size=20")
assert url.options["ssl"] == "true"
assert url.options["pool_size"] == "20"
```

### Normalize with backend rules

```python
from databasez import DatabaseURL

# Applies overwrite extraction rules for the target dialect.
upgraded = DatabaseURL("mysql://localhost/app").upgrade()
```

## Notes

- `repr(DatabaseURL(...))` obscures passwords.
- `hostname` supports host values passed in query options.
- `replace(options=...)` writes query parameters back into the URL.

For constructor/config usage examples see [Database](./database.md).
