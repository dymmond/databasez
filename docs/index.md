# Databasez

<p align="center">
  <a href="https://databasez.dymmond.com"><img src="https://res.cloudinary.com/dymmond/image/upload/v1680611626/databasez/logo-cmp_luizb0.png" alt='databasez'></a>
</p>

<p align="center">
    <em>🚀 Async database support for Python. 🚀</em>
</p>

<p align="center">
<a href="https://github.com/dymmond/databasez/workflows/Test%20Suite/badge.svg?event=push&branch=main" target="_blank">
    <img src="https://github.com/dymmond/databasez/workflows/Test%20Suite/badge.svg?event=push&branch=main" alt="Test Suite">
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

A lot of packages depends of Databases and this was the main reason for the fork of **Databasez**.

It allows you to make queries using the powerful [SQLAlchemy Core][sqlalchemy-core]
expression language, and provides support for PostgreSQL, MySQL, SQLite and MSSQL.

Databasez is suitable for integrating against any async Web framework, such as [Esmerald][esmerald],
[Starlette][starlette], [Sanic][sanic], [Responder][responder], [Quart][quart], [aiohttp][aiohttp],
[Tornado][tornado], or [FastAPI][fastapi].

Databasez was built for Python 3.8+ and on the top of the newest **SQLAlchemy 2** and gives you
simple asyncio support for a range of databases.

### Special notes
This package couldn't exist without [Databases](https://www.encode.io/databases/) and the continuous work
done by the amazing team behind it. For that reason, thank you!

## Installation

```shell
$ pip install databasez
```

If you are interested in using the [test client](./test-client.md), you can just use it.

## What does databasez support at the moment

Databasez currently supports nearly all async drivers of sqlalchemy.

If this is not enough there are two special dialects with restricted features:

- jdbc: can load nearly any jdbc driver
- dbapi2: can load nearly any dbapi2 driver (python standard), async as well as sync.

### Driver installation

You can install the required database drivers with:

#### Postgres

```shell
$ pip install databasez[asyncpg]
```


#### MySQL/MariaDB

```shell
$ pip install databasez[aiomysql]
```

or

```shell
$ pip install databasez[asyncmy]
```

#### SQLite

```shell
$ pip install databasez[aiosqlite]
```

#### MSSQL

```shell
$ pip install databasez[aioodbc]
```

!!! Note
    MSSQL and odbc may segfaults with threading (sometimes but in tests it can be more often).

---

## Quickstart

For a simple quickstart example, we will be creating a simple SQLite database to run some queries
against.

First, install the required drivers for `SQLite` and `ipython`. The `ipython` is to have an
interactive python shell with some extras. IPython also supports `await`, which is exactly
what we need. [See more details](https://ipython.org/) about it.

**Install the required drivers**

```shell
$ pip install databases[aiosqlite]
$ pip install ipython
```

Now from the console, we can run a simple example.


```python
{!> ../docs_src/quickstart/quickstart.py !}
```

Check out the documentation on [making database queries](./queries.md)
for examples of how to start using databases together with SQLAlchemy core expressions.

For the integration in frameworks see:

[Integrations](./integrations.md)


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

[esmerald]: https://github.com/dymmond/esmerald
[starlette]: https://github.com/encode/starlette
[sanic]: https://github.com/huge-success/sanic
[responder]: https://github.com/kennethreitz/responder
[quart]: https://gitlab.com/pgjones/quart
[aiohttp]: https://github.com/aio-libs/aiohttp
[tornado]: https://github.com/tornadoweb/tornado
[fastapi]: https://github.com/tiangolo/fastapi
