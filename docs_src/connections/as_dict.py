from databasez import Database

CONFIG = {
    "connection": {
        "credentials": {
            "scheme": "postgres+asyncpg",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "password",
            "database": "my_db",
        }
    }
}

database = Database(config=CONFIG)
