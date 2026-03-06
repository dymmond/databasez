# Queries

Databasez supports queries using SQLAlchemy Core and raw SQL.

## Table declarations

To use SQLAlchemy Core queries, define your metadata and tables first.

```python
{!> ../docs_src/queries/declarations.py !}
```

Another example with JSON columns:

```python
{!> ../docs_src/queries/declarations2.py !}
```

## Creating tables

Databasez provides `create_all` and `drop_all` helpers that call SQLAlchemy metadata operations through the active connection.

You can also compile DDL manually when needed.

```python
{!> ../docs_src/queries/create_tables.py !}
```

!!! Note
    For production projects, use a migration tool such as [Alembic](https://alembic.sqlalchemy.org/en/latest/).

## SQLAlchemy Core queries

```python
{!> ../docs_src/queries/queries.py !}
```

## Raw SQL queries

```python
{!> ../docs_src/queries/raw_queries.py !}
```

!!! Tip
    Use named bind parameters with `:param_name` style.

## Iteration and batching

- `iterate(...)` yields row-by-row.
- `batched_iterate(...)` yields batches.
- `batch_wrapper` can transform each batch (`tuple`, `list`, custom callable).

## Timeouts

Most query methods accept `timeout=...`.

For cross-loop debug timeout, use:

`databasez.utils.DATABASEZ_RESULT_TIMEOUT`

## Related pages

- [Database](./database.md)
- [Connections & Transactions](./connections-and-transactions.md)
- [Extra drivers and overwrites](./extra-drivers-and-overwrites.md)
