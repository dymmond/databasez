# Databasez

<p align="center">
  <a href="https://databasez.dymmond.com"><img src="https://res.cloudinary.com/dymmond/image/upload/v1680611626/databasez/logo-cmp_luizb0.png" alt='databasez'></a>
</p>

<p align="center">
    <em>🚀 Async database support for Python. 🚀</em>
</p>

<p align="center">
<a href="https://github.com/dymmond/databasez/actions/workflows/test-suite.yml/badge.svg?event=push&branch=main" target="_blank">
    <img src="https://github.com/dymmond/databasez/actions/workflows/test-suite.yml/badge.svg?event=push&branch=main" alt="Test Suite">
</a>

<a href="https://pypi.org/project/databasez" target="_blank">
    <img src="https://img.shields.io/pypi/v/databasez?color=%2334D058&label=pypi%20package" alt="Package version">
</a>

<a href="https://pypi.org/project/databasez" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/databasez.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

---

**Documentation**: [https://databasez.dymmond.com](https://databasez.dymmond.com) 📚

**Source Code**: [https://github.com/dymmond/databasez](https://github.com/dymmond/databasez)

---

## Motivation

There is a great package from [Encode](https://github.com/encode/databases/) that was doing what
this package was forked to do.

From a need to extend to new drivers and newest technologies and adding extra features common and
useful to the many, [Databases](https://github.com/encode/databases/) was forked to become
**Databasez**.

This package is 100% backwards compatible with [Databases](https://github.com/encode/databases/)
from Encode and will remain like this for the time being but adding extra features and regular
updates as well as continuing to be community driven.

By the time this project was created, Databases was yet to merge possible SQLAlchemy 2.0 changes
from the author of this package and therefore, this package aims to unblock
a lot of the projects out there that want SQLAlchemy 2.0 with the best of databases with new features.

A lot of packages depend on Databases and this was the main reason for the fork of **Databasez**.

Databasez was built for Python 3.10+ and on top of **SQLAlchemy 2** and gives you
simple asyncio support for a range of databases.

### Special notes

This package couldn't exist without [Databases](https://www.encode.io/databases/) and the continuous work
done by the amazing team behind it. For that reason, thank you.

## Installation

```shell
$ pip install databasez
```

Optional driver extras:

- PostgreSQL: `pip install databasez[postgresql]` or `databasez[asyncpg]`
- MySQL/MariaDB: `pip install databasez[mysql]` or `databasez[aiomysql]`
- SQLite: `pip install databasez[sqlite]`
- MSSQL: `pip install databasez[aioodbc]`
- JDBC support: `pip install databasez[jdbc]`

!!! Note
    MSSQL and ODBC drivers may occasionally segfault in threaded test environments.

## What databasez supports

Databasez currently supports nearly all async SQLAlchemy drivers.

If this is not enough there are two extra dialects with restricted features:

- `jdbc`: can load many JDBC drivers.
- `dbapi2`: can load many DBAPI2 drivers (sync and async).

See [Extra drivers and overwrites](./extra-drivers-and-overwrites.md) for details.

## Quickstart

For a simple quickstart example, we create a SQLite database and run a few operations.

```python
{!> ../docs_src/quickstart/quickstart.py !}
```

## Recommended reading order

1. [Core Concepts](./core-concepts.md)
2. [Database](./database.md)
3. [Queries](./queries.md)
4. [Connections & Transactions](./connections-and-transactions.md)
5. [Integrations](./integrations.md)

## Architecture and internals

- [Architecture Overview](./architecture-overview.md)
- [Database URL](./database-url.md)
- [Troubleshooting](./troubleshooting.md)

[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-core-tutorial]: https://docs.sqlalchemy.org/en/latest/core/tutorial.html
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[psycopg]: https://www.psycopg.org/
[pymysql]: https://github.com/PyMySQL/PyMySQL
[pyodbc]: https://github.com/mkleehammer/pyodbc
[asyncpg]: https://github.com/MagicStack/asyncpg
[aiomysql]: https://github.com/aio-libs/aiomysql
[asyncmy]: https://github.com/long2ice/asyncmy
[aiosqlite]: https://github.com/omnilib/aiosqlite
[aioodbc]: https://aioodbc.readthedocs.io/en/latest/
