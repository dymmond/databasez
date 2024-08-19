# Create a database instance, and connect to it.
import copy

from databasez import Database

# non captured arguments are passed through to create_async_engine
database = Database("sqlite+aiosqlite:///example.db", echo=True)
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

print("High Scores:", rows)

# new Database object with the same parameters required? No problem, we can copy:
database2 = copy.copy(database)
# or
database2 = Database(database)
await database2.connect()
await database2.disconnect()
assert database.is_connected == True
