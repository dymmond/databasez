from databasez import Database

CONFIG = {
    "connection": {
        "credentials": {
            "scheme": "postgresql+asyncpg",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "password",
            "database": "my_db",
            "options": {"ssl": "true", "pool_size": "5"},
        }
    }
}

database = Database(config=CONFIG)
