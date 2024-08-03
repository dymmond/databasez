import sqlalchemy

from databasez import Database

database = Database("postgresql+asyncpg://localhost/example")

# Establish the connection pool
await database.connect()

metadata = sqlalchemy.MetaData()
dialect = sqlalchemy.dialects.postgresql.dialect()

# Define your table(s)
users = sqlalchemy.Table(
    "users",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=150)),
    sqlalchemy.Column("address", sqlalchemy.String(length=500)),
)

# Create tables (manually)
for table in metadata.tables.values():
    # Set `if_not_exists=False` if you want the query to throw an
    # exception when the table already exists
    schema = sqlalchemy.schema.CreateTable(table, if_not_exists=True)
    query = str(schema.compile(dialect=dialect))
    await database.execute(query=query)

# Create tables automatic
await database.create_all(metadata)

# Close all connections in the connection pool
await database.disconnect()
