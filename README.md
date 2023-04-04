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
this package was initially forked to do but Encode is also very busy with other projects and
without the proper time to maintain the so much required package.

From a need to extend to new drivers and newest technologies and adding extra features common and
useful to the many, [Databases](https://github.com/encode/databases/) was forked to become
**Databasez**.

This package is 100% backwards compatible with [Databases](https://github.com/encode/databases/)
from Encode and will remain like this for the time being but adding extra features and regular
updates as well as continuing to be community driven.

A lot of packages depends of Databases and this was the main reason for the fork of **Databasez**.
The need of progressing.

Databasez was built to with Python 3.8+ and on the top of the newest SQLAlchemy 2.0.

## Installation

```shell
$ pip install databasez
```

Database drivers supported are:

* [asyncpg][asyncpg]
* [aiopg][aiopg]
* [aiomysql][aiomysql]
* [asyncmy][asyncmy]
* [aiosqlite][aiosqlite]
* [aioodbc][aioodbc]

You can install the required database drivers with:

```shell
$ pip install databases[asyncpg]
$ pip install databases[aiopg]
$ pip install databases[aiomysql]
$ pip install databases[asyncmy]
$ pip install databases[aiosqlite]
$ pip install databases[aioodbc]
```

Note that if you are using any synchronous SQLAlchemy functions such as `engine.create_all()` or [alembic][alembic] migrations then you still have to install a synchronous DB driver: [psycopg2][psycopg2] for PostgreSQL, [pymysql][pymysql] for MySQL and [pyodbc][pyodbc] for SQL Server.

---

## Quickstart

For this example we'll create a very simple SQLite database to run some
queries against.

```shell
$ pip install databases[aiosqlite]
$ pip install ipython
```

We can now run a simple example from the console.

Note that we want to use `ipython` here, because it supports using `await`
expressions directly from the console.

```python
# Create a database instance, and connect to it.
from databases import Database
database = Database('sqlite+aiosqlite:///example.db')
await database.connect()

# Create a table.
query = """CREATE TABLE HighScores (id INTEGER PRIMARY KEY, name VARCHAR(100), score INTEGER)"""
await database.execute(query=query)

# Insert some data.
query = "INSERT INTO HighScores(name, score) VALUES (:name, :score)"
values = [
    {"name": "Daisy", "score": 92},
    {"name": "Neil", "score": 87},
    {"name": "Carol", "score": 43},
]
await database.execute_many(query=query, values=values)

# Run a database query.
query = "SELECT * FROM HighScores"
rows = await database.fetch_all(query=query)
print('High Scores:', rows)
```

Check out the documentation on [making database queries](https://www.encode.io/databases/database_queries/)
for examples of how to start using databases together with SQLAlchemy core expressions.


<p align="center">&mdash; ⭐️ &mdash;</p>
<p align="center"><i>Databases is <a href="https://github.com/encode/databases/blob/master/LICENSE.md">BSD licensed</a> code. Designed & built in Brighton, England.</i></p>

[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-core-tutorial]: https://docs.sqlalchemy.org/en/latest/core/tutorial.html
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[psycopg2]: https://www.psycopg.org/
[pymysql]: https://github.com/PyMySQL/PyMySQL
[pyodbc]: https://github.com/mkleehammer/pyodbc
[asyncpg]: https://github.com/MagicStack/asyncpg
[aiopg]: https://github.com/aio-libs/aiopg
[aiomysql]: https://github.com/aio-libs/aiomysql
[asyncmy]: https://github.com/long2ice/asyncmy
[aiosqlite]: https://github.com/omnilib/aiosqlite
[aioodbc]: https://aioodbc.readthedocs.io/en/latest/

[starlette]: https://github.com/encode/starlette
[sanic]: https://github.com/huge-success/sanic
[responder]: https://github.com/kennethreitz/responder
[quart]: https://gitlab.com/pgjones/quart
[aiohttp]: https://github.com/aio-libs/aiohttp
[tornado]: https://github.com/tornadoweb/tornado
[fastapi]: https://github.com/tiangolo/fastapi