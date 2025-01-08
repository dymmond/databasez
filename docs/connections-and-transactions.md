# Connections and Transactions

## Connections

Connections are the heart-piece of databasez. They are enabling rollback behavior, via transactions
and provide all manipulation functions.
Despite there are shortcut methods on the [Database object](./database.md), they are all using
connections internally.
When wanting for performance reasons one connection doing multiple jobs, retrieving a connection via
`connection()` and perform all tasks within the connection context (connections are async contextmanagers) are the way to go.

### Multithreading

`sqlalchemy`'s async addon is also multi-threading capable but we cannot use this for two reasons:

- Only the `NullPool` is supported which is afaik just no pooling.
- No global connection which is reliable resetted would be possible.

Therefor `databasez` uses an other approach: `isolation`:

Event-loops are per thread. So in this approach for every new event-loop detected the database object is copied and assigned to it.
This way all connections are running in a per-thread pool. The global connection is a bit different from the rest.
Whenever it is accessed by an other thread, asyncio.run_coroutine_threadsafe is used.
In the full-isolation mode (default) the global connection is even moved to an own thread.

### Global connection with auto-rollback

Sometimes, especially for tests, you want a connection in which all changes are resetted when the database is closed.
For having this reliable, a global connection is lazily initialized when requested on the database object via the
`force_rollback` contextmanager/parameter/pseudo-attribute.

Whenever `connection()` is called, the same global connection is returned. It behaves nearly normally except you have just one connection
available.
This means you have to be careful when using `iterate` to not open another connection via `connection` (except you set `force_rollback` to False before opening the connection).
Otherwise it will deadlock.

Example for `force_rollback`:

```python
{!> ../docs_src/connections/force_rollback.py !}
```

## Transactions

Transactions are lazily initialized. You dont't need a connected database to create them via `database.transaction()`.

When created, there are three ways to activate the Transaction

1. The async context manager way via `async with transaction: ...`
2. Entering a method decorated with a transaction.
3. The manual way via `await transaction`

Whenever the transaction is activated, a connected database is required.

A second way to get a transaction is via the `Connection`. It has also a `transaction()` method.


### The three ways to use a transaction

#### Via the async context manager protocol

```python
{!> ../docs_src/transactions/async_context_database.py !}
```
It can also be acquired from a specific database connection:

```python
{!> ../docs_src/transactions/async_context_connection.py !}
```

#### Via decorating

You can also use `.transaction()` as a function decorator on any async function:

```python
{!> ../docs_src/transactions/decorating.py !}
```

#### Manually

For a lower-level transaction API:

```python
{!> ../docs_src/transactions/manually.py !}
```

### Auto-rollback (`force_rollback`)

Transactions support an keyword parameter named `force_rollback` which default to `False`.
When set to `True` at the end of the transaction a rollback is tried.
This means all changes are undone.

This is a simpler variant of `force_rollback` of the database object.
```python
{!> ../docs_src/transactions/force_rollback_transaction.py !}
```


### Isolation level

The state of a transaction is liked to the connection used in the currently executing async task.
If you would like to influence an active transaction from another task, the connection must be
shared:

Transaction isolation-level can be specified if the driver backend supports that:

```python
{!> ../docs_src/transactions/isolation_level.py !}
```

### Nested transactions

Nested transactions are fully supported, and are implemented using database savepoints:

```python
{!> ../docs_src/transactions/nested_transactions.py !}
```
