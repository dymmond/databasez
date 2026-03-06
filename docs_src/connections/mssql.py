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
            "options": {
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
                "Encrypt": "Optional",
            },
        }
    }
}

database = Database(config=CONFIG)
