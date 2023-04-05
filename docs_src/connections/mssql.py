from databasez import Database

CONFIG = {
    "connection": {
        "credentials": {
            "scheme": "mssql+aioodbc",
            "host": "localhost",
            "port": 1433,
            "user": "sa",
            "password": "Mssql123mssql",
            "database": "master",
            "options": {"driver": "ODBC Driver 17 for SQL Server"},
        }
    }
}

database = Database(config=CONFIG)
