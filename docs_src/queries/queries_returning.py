from collections.abc import Sequence

import sqlalchemy

from databasez import Database

database = Database("postgresql+asyncpg://localhost/example")


# Establish the connection pool
await database.connect()


metadata = sqlalchemy.MetaData()
# Define your table(s)
users = sqlalchemy.Table(
    "users",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=150)),
    sqlalchemy.Column("address", sqlalchemy.String(length=500)),
)

# Execute
query = users.insert()
if database.engine.dialect.insert_returning:
    query = query.returning(users.columns.name)
values = {"name": "databasez", "address": "London, United Kingdom"}
result = await database.execute(query=query, values=values)
if database.engine.dialect.insert_returning:
    # do something with name
    assert result.name == "databasez"
else:
    # row count
    assert result == 1

# Execute many
query = users.insert()
if database.engine.dialect.insert_executemany_returning:
    query = query.returning(users.columns.name)
values = [
    {"name": "databasez2", "address": "London, United Kingdom"},
    {"name": "another name", "address": "The Hague, Netherlands"},
]
results = await database.execute_many(query=query, values=values)
if database.engine.dialect.insert_returning:
    # do something with name
    assert results[0].name == "databasez2"
elif isinstance(results, Sequence):
    # do something with the inserted pks
    assert results[0].id
else:
    # row number
    assert results == 2

# fence the same way updates for portability
if database.engine.dialect.update_returning:
    print("yeah single value updates with returning are supported")

if database.engine.dialect.update_executemany_returning:
    print("yeah multiple value updates with returning are supported")

# Close all connections in the connection pool
await database.disconnect()
