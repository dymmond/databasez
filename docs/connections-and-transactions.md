# Connections and Transactions

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

* **config** - A python like dictionary as alternative to the `url` that contains the information
to connect to the database.

  <sup>Default: `None`</sup>

* **options** - Any other configuration. The classic `kwargs`.

!!! Warning
    Be careful when setting up the `url` or `config`. You can use one or the other but not both
    at the same time.


**Attributes***

* **force_rollback**:
    It evaluates its trueness value to the active value of force_rollback for this context.
    You can delete it to reset it (`del database.force_rollback`) (it uses the descriptor magic).

**Functions**

* **__copy__** - Either usable directly or via copy from the copy module. A fresh Database object with the same options as the existing is created.
                 Note: for creating a copy with overwritten initial force_rollback you can use: `Database(database_obj, force_rollback=False)`.
                 Note: you have to connect it.

* **force_rollback(force_rollback=True)**: - The magic attribute is also function returning a context-manager for temporary overwrites of force_rollback.


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
[Esmerald][esmerald] you would use the following:

```python
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
```

Starlette is deprecating the previous way of declaring events in favour of the newly created
`Lifespan`.

[Esmerald][esmerald] on the other hand, will allow you to continue to use the events in both ways.
Internally Esmerald handles the `on_startup` and `on_shutdown` in a unique and clean way where
it will automatically generate the `Lifespan` events for you.

!!! Note
    Since the author of [Esmerald][esmerald] and [Edgy][edgy] is the same and you would like to
    have those same type of events to continue also for Starlette even after the deprecation and without
    breaking your code or diverge from Starlette's base, the same author is the creator of
    [Starlette Bridge][starlette-bridge] where it was shared the same approach created for Esmerald with
    everyone to use with Starlette or any Starlette related project, for example, FastAPI.


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
* Password: `Mssql123mssql`
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

## JDBC

Databasez injects a jdbc driver. You can use it as simple as:

`jdbc+jdbc-dsn-driver-name://dsn?jdbc_driver=?`

or for modern jdbc drivers

`jdbc+jdbc-dsn-driver-name://dsn`

Despite the jdbc-dsn-driver-name is not known by sqlalchemy this works. The overwrites rewrite the URL for sqlalchemy.


!!! Warning
    It seems like injecting classpathes in a running JVM doesn't work properly. If you have more then one jdbc driver,
    make sure all classpathes are specified.


!!! Warning
    The jdbc driver doesn't support setting the isolation_level yet (this is highly db vendor specific).

### Parameters

The jdbc driver supports some extra parameters which will be removed from the query (note: most of them can be also passed via keywords)

* **jdbc_driver** - import path of the jdbc driver (java format). Required for old jdbc drivers.
* **jdbc_driver_args** - additional keyword arguments for the driver. Note: they must be passed in json format. Query only parameter.
* **jdbc_dsn_driver** - Not really required because of the rewrite but if the url only specifies jdbc:// withouth the dsn driver you can set it here manually.


## dbapi2


Databasez injects a dbapi2 driver. You can use it as simple as:

`dbapi2+foo://dsn`

or simply

`dbapi2://dsn`

!!! Warning
    The dbapi2 driver doesn't support setting the isolation_level yet (this is highly db vendor specific).

### Parameters

The dbapi2 driver supports some extra parameters which will be removed from the query (note: most of them can be also passed via keywords)

* **dbapi_driver_args** - additional keyword arguments for the driver. Note: they must be passed in json format. Query only parameter.
* **dbapi_dsn_driver** - If required it is possible to set the dsn driver here. Normally it should work without. You can use the same trick like in jdbc to provide a dsn driver.
* **dbapi_pool** - thread/process. Default: thread. How the dbapi2. is isolated. Either via ProcessPool or ThreadPool (with one worker).
* **dbapi_force_async_wrapper** - bool/None. Default: None. Figure out if the async_wrapper is required. By setting a bool the wrapper can be enforced or removed.

The dbapi2 has some extra options which cannot be passed via the url, some of them are required:

* **dbapi_path** - Import path of the dbapi2 module. Required

## Transactions

Transactions are managed by async context blocks.

A transaction can be acquired from the database connection pool:

```python
async with database.transaction():
    ...
```
It can also be acquired from a specific database connection:

```python
async with database.connection() as connection:
    async with connection.transaction():
        ...
```

For a lower-level transaction API:

```python
transaction = await database.transaction()
try:
    ...
except:
    await transaction.rollback()
else:
    await transaction.commit()
```

You can also use `.transaction()` as a function decorator on any async function:

```python
@database.transaction()
async def create_users(request):
    ...
```

The state of a transaction is liked to the connection used in the currently executing async task.
If you would like to influence an active transaction from another task, the connection must be
shared:

Transaction isolation-level can be specified if the driver backend supports that:

```python
async def add_excitement(connnection: databases.core.Connection, id: int):
    await connection.execute(
        "UPDATE notes SET text = CONCAT(text, '!!!') WHERE id = :id",
        {"id": id}
    )


async with Database(database_url) as database:
    async with database.transaction():
        # This note won't exist until the transaction closes...
        await database.execute(
            "INSERT INTO notes(id, text) values (1, 'databases is cool')"
        )
        # ...but child tasks can use this connection now!
        await asyncio.create_task(add_excitement(database.connection(), id=1))

    await database.fetch_val("SELECT text FROM notes WHERE id=1")
    # ^ returns: "databases is cool!!!"
```

Nested transactions are fully supported, and are implemented using database savepoints:

```python
async with databases.Database(database_url) as db:
    async with db.transaction() as outer:
        # Do something in the outer transaction
        ...

        # Suppress to prevent influence on the outer transaction
        with contextlib.suppress(ValueError):
            async with db.transaction():
                # Do something in the inner transaction
                ...

                raise ValueError('Abort the inner transaction')

    # Observe the results of the outer transaction,
    # without effects from the inner transaction.
    await db.fetch_all('SELECT * FROM ...')
```

```python
async with database.transaction(isolation_level="serializable"):
    ...
```

## Reusing sqlalchemy engine of databasez

For integration in other libraries databasez has also the AsyncEngine exposed via the `engine` property.
If a database is connected you can retrieve the engine from there.

## Special jdbc/dbapi2 stuff

Currently there is not much documentation and you have to check the overwrites and dialects yourself to get an idea how it works.
Additional there is a jdbc test with a really old sqlite jdbc jar so you may get an idea how it works.

However it is planned to update the documentation.

[esmerald]: https://github.com/dymmond/esmerald
[edgy]: https://github.com/dymmond/edgy
[saffier]: https://github.com/tarsil/saffier
[starlette-bridge]: https://github.com/tarsil/starlette-bridge
