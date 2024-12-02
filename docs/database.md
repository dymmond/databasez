# Connecting to a database

Databasez handles database connection pooling and transaction management
with minimal fuss. It'll automatically deal with acquiring and releasing
connections to the pool as needed, and supports a simple transaction API
that transparently handles the use of either transactions or savepoints.

## Database

This is the main object used for the connections and it is a very powerful object.

```python
from databasez import Database
```

**Parameters**

* **url** - The `url` of the connection string or a Database object to copy from.

  <sup>Default: `None`</sup>

* **force_rollback** - An optional boolean flag for force_rollback. Overwritable at runtime possible.
                       Note: when None it copies the value from the provided Database object or sets it to False.

  <sup>Default: `None`</sup>

* **full_isolation** - Special mode for using force_rollback with nested queries. This parameter fully isolates the global connection
                       in an extra thread. This way it is possible to use blocking operations like locks with force_rollback.
                       This parameter has no use when used without force_rollback and causes a slightly slower setup (Lock is initialized).
                       It is required for edgy or other frameworks which use threads in tests and the force_rollback parameter.

  <sup>Default: `None`</sup>

* **poll_interval** - When using multithreading, the poll_interval is used to retrieve results from other loops. It defaults to a sane value.

  <sup>Default: `None`</sup>

* **config** - A python like dictionary as alternative to the `url` that contains the information
to connect to the database.

  <sup>Default: `None`</sup>

* **options** - Any other configuration. The classic `kwargs`.

!!! Warning
    Be careful when setting up the `url` or `config`. You can use one or the other but not both
    at the same time.

!!! Warning
    `full_isolation` is not mature and shouldn't be used in production code.

**Attributes***

* **force_rollback**:
    It evaluates its trueness value to the active value of force_rollback for this context.
    You can delete it to reset it (`del database.force_rollback`) (it uses the descriptor magic).

**Functions**

* **__copy__** - Either usable directly or via copy from the copy module. A fresh Database object with the same options as the existing is created.
                 Note: for creating a copy with overwritten initial force_rollback you can use: `Database(database_obj, force_rollback=False)`.
                 Note: you have to connect it.

* **force_rollback(force_rollback=True)**: - The magic attribute is also function returning a context-manager for temporary overwrites of force_rollback.

* **asgi** - ASGI lifespan interception shim.

## Connecting and disconnecting

You can control the database connection, by using it as a async context manager.

```python
async with Database(DATABASE_URL) as database:
    ...
```

Or by using explicit `.connect()` and `disconnect()`:

```python
database = Database(DATABASE_URL)

await database.connect()
...
await database.disconnect()
```

If you're integrating against a web framework, then you'll probably want
to hook into framework startup or shutdown events. For example, with
[Esmerald][esmerald] you could use the following:

```python
{!> ../docs_src/integrations/django.py !}
```

In django/
## Connection options as a string

The PostgreSQL and MySQL backends provide a few connection options for SSL
and for configuring the connection pool.

```python
# Use an SSL connection.
database = Database('postgresql+asyncpg://localhost/example?ssl=true')

# Use an SSL connection and configure pool
database = Database('postgresql+asyncpg://localhost/example?ssl=true&pool_size=20')
```

You can also use keyword arguments to pass in any connection options.
Available keyword arguments may differ between database backends. Keywords can be used like in create_async_engine (most are passed through).
This means also the keyword extraction works like in sqlalchemy

```python
database = Database('postgresql+asyncpg://localhost/example', ssl=True, pool_size=20)
```

Note: not all query values are morphed into kwargs arguments.
Options which will be transformed are in `databasez/sqlalchemy.py` in `extract_options`

Some transformed options are:

- ssl: enable ssl.
- echo: enable echo.
- echo_pool: enable echo for pool.
- pool_size: maximal amount of connections, int (former name: min_size).
- max_overflow: maximal amount of connections, int (former name: max_size).
- pool_recycle: maximal duration a connection may live, float.
- isolation_level: isolation_level, str.


## Connection options as a dictionary

**Databasez** also provides another way of passing the connection options by using dictionaries.

For those who are used to other frameworks handling the connections in this way, this was also a
reason why this was also added.

Databasez expects a python dictionary like object with the following structure.

```python

"connection": {
    "credentials": {
        "scheme": 'sqlite', "postgres" ...
        "host": ...,
        "port": ...,
        "user": ...,
        "password": ...,
        "database": ...,
        "options": {  # only query
            "driver": ... # In case of MSSQL
            "ssl": ...
        }
    }
}
```

When a python dictionary like is passed into the `Database` object, then the `config` parameter
needs to be set.

```python
from databasez import Database

CONFIG = {
    "connection": {
        "credentials": {
             "scheme": 'sqlite', "postgres" ...
             "host": ...,
             "port": ...,
             "user": ...,
             "password": ...,
             "database": ...,
             "options": {
             "driver": ... # In case of MSSQL
             "ssl": ...
            }
        }
    }
}

database = Database(config=CONFIG)
```

The `options` **is everything else that should go in the query parameters, meaning, after the `?`**

### Normal cases

Let us see an example. Let us assume we have a database with the following:

* Type: `postgres`
* Database name: `my_db`
* User: `postgres`
* Password: `password`
* Port: `5432`
* Host: `localhost`

This would look like this:

```python
{! ../docs_src/connections/as_dict.py!}
```

This is the equivalent to:

```shell
postgresql+asyncpg://postgres:password@localhost:5432/my_db
```

### A more complex example

Let us now use an example using `MSSQL` which usually requires more options to be passed.

* Type: `mssql`
* Database name: `master`
* User: `sa`
* Password: `Mssql123mssql-`
* Port: `1433`
* Host: `localhost`

This would look like this:

```python
{! ../docs_src/connections/mssql.py!}
```

This is the equivalent to:

```shell
mssql+aioodbc://sa:Mssql123mssql-@localhost:1433/master?driver=ODBC+Driver+17+for+SQL+Server
```

!!! Note
    As you can see, Databasez offers some other ways of achieving the same results and offers
    multiple forms of creating a [Database](#database) object.


## Extra drivers

Databasez comes with extra drivers and overwrites for existing drivers.
That way some strange defaults of sqlalchemy are smoothed and the DSN analysed for extra parameters
which were keyword only in the original dialect.
This way an integration is easier.

[Extra drivers and overwrites](./extra-drivers-and-overwrites.md)

## Connections and Transactions

Databasez uses enhanced connection objects. They smooth out some API decisions by the sqlalchemy authors
but allow direct access to the sqlalchemy connections too.
So both styles can be mixed.

Most important is, only when using them as context, operations are guranteed async/multithreading capable when using databasez.

Further documentation is in:

[Connections & Transactions](./connections-and-transactions.md)


## Reusing sqlalchemy engine of databasez

For integration in other libraries databasez has also the AsyncEngine exposed via the `engine` property.
If a database is connected you can retrieve the engine from there.
It is however protected, so you can only access it from the same loop.

## Debugging (multithreading)

Sometimes there is a lockup. To get of the underlying issues, you can set

`databasez.utils.DATABASEZ_RESULT_TIMEOUT` to a positive float/int value.

This way lockups will raise an exception.

## Links

[esmerald]: https://github.com/dymmond/esmerald
