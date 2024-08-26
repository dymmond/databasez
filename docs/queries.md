# Queries

Databasez supports queries using SQLAlchemy core as well as raw SQL. This means you can take
advantage or the full power of the package as well as be free to do your own custom queries.

## Declarations

To be able to make queries using the SQLAlchemy core you will need to create/declare your tables
in your codebase using the proper syntax.

It is good practice in any case as this make it a lot easier to keep your database schema in sync
with the code that is accessing it. This also allows to use a database migration tool of choice to
manage any schema change.

```python
{!> ../docs_src/queries/declarations.py !}
```

Declaring a database table using SQLAlchemy core is very simple and it should follow the best
practices mentioned by the author. You can create as many tables and columns as you wish.

Regarding the columns, you can use any of the SQLAlchemy column types such as `sqlalchemy.JSON`
or a custom column type.

Another example could be:

```python hl_lines="11"
{!> ../docs_src/queries/declarations2.py !}
```

## Creating tables

As per fork of Databases, **Databasez** also does not use SQLAlchemy's engine for database access
internally. [The usual SQLAlchemy core way to create tables with `create_all`](https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.MetaData.create_all)
is therefore not available.

To circunvent this situation you can use SQLAlchemy to [compile the query to SQL](https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html#how-do-i-render-sql-expressions-as-strings-possibly-with-bound-parameters-inlined) and then
execute it with databasez.

```python
{!> ../docs_src/queries/create_tables.py !}
```

!!! Note
    Note that this is way of creating tables is only useful for local testing and experimentation.
    For big and serious projects, you should be using something like
    [Alembic](https://alembic.sqlalchemy.org/en/latest/) or any proper migration solution tool.

## Queries

Since you can use [SQLAlchemy core](https://docs.sqlalchemy.org/en/20/core/), that also means you
can also use the queries. Check out the [official tutorial](https://docs.sqlalchemy.org/en/20/tutorial/).

```python
{!> ../docs_src/queries/queries.py !}
```

Connections are managed as task-local state, with driver implementations using connection pooling
behind the scenes.

## Raw Queries

```python
{!> ../docs_src/queries/raw_queries.py !}
```

!!! Tip
    The query arguments should follow the `:query_arg` style.

Databasez allows the same level of flexibility as per its ancestor and adds its own extra flavours
on the top.
