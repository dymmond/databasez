from databasez import Database

database = Database("postgresql+asyncpg://localhost/example")


# Establish the connection pool
await database.connect()

# Execute
query = "INSERT INTO users(name, address) VALUES (:name, :address)"
values = {"text": "databasez", "address": "London, United Kingdom"}
await database.execute(query=query, values=values)

# Execute many
query = "INSERT INTO users(name, address) VALUES (:name, :address)"
values = [
    {"name": "databasez", "address": "London, United Kingdom"},
    {"name": "another name", "address": "The Hague, Netherlands"},
]
await database.execute_many(query=query, values=values)

# Fetch multiple rows
query = "SELECT * FROM users WHERE address = :address"
rows = await database.fetch_all(query=query, values={"address": "London, United Kingdom"})

# Fetch single row
query = "SELECT * FROM users WHERE id = :id"
result = await database.fetch_one(query=query, values={"id": 1})
