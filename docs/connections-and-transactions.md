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

* **url** - The `url` of the connection string.
  
  <sup>Default: `None`</sup>

* **force_rollback** - A boolean flag indicating if it should force the rollback.

  <sup>Default: `False`</sup>

* **config** - A python like dictionary as alternative to the `url` that contains the information
to connect to the database.

  <sup>Default: `None`</sup>

* **options** - Any other configuration. The classic `kwargs`.

!!! Warning
    Be careful when setting up the `url` or `config`. You can use one or the other but not both
    at the same time.


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

# Use a connection pool of between 5-20 connections.
database = Database('mysql+aiomysql://localhost/example?min_size=5&max_size=20')
```

You can also use keyword arguments to pass in any connection options.
Available keyword arguments may differ between database backends.

```python
database = Database('postgresql+asyncpg://localhost/example', ssl=True, min_size=5, max_size=20)
```

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
        "options": {
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
