"""
Unit tests for the backend connection arguments.
"""

from databasez.backends.asyncmy import AsyncMyBackend
from databasez.backends.mysql import MySQLBackend
from databasez.backends.postgres import PostgresBackend
from databasez.core import DatabaseURL
from tests.test_databases import DATABASE_URLS, async_adapter


def test_postgres_pool_size():
    backend = PostgresBackend("postgres://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


@async_adapter
async def test_postgres_pool_size_connect():
    for url in DATABASE_URLS:
        if DatabaseURL(url).dialect != "postgresql":
            continue
        backend = PostgresBackend(url + "?min_size=1&max_size=20")
        await backend.connect()
        await backend.disconnect()


def test_postgres_explicit_pool_size():
    backend = PostgresBackend("postgres+psycopg://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


def test_postgres_ssl():
    backend = PostgresBackend("postgres+psycopg://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_postgres_explicit_ssl():
    backend = PostgresBackend("postgres+psycopg://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_postgres_no_extra_options():
    backend = PostgresBackend("postgres+psycopg://localhost/database")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {}


def test_postgres_password_as_callable():
    def gen_password():
        return "Foo"

    backend = PostgresBackend("postgres://:password@localhost/database", password=gen_password)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"password": gen_password}
    assert kwargs["password"]() == "Foo"


def test_mysql_pool_size():
    backend = MySQLBackend("mysql://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mysql_unix_socket():
    backend = MySQLBackend(
        "mysql+aiomysql://username:password@/testsuite?unix_socket=/tmp/mysqld/mysqld.sock"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"unix_socket": "/tmp/mysqld/mysqld.sock"}


def test_mysql_explicit_pool_size():
    backend = MySQLBackend("mysql://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mysql_ssl():
    backend = MySQLBackend("mysql://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_mysql_explicit_ssl():
    backend = MySQLBackend("mysql://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_mysql_pool_recycle():
    backend = MySQLBackend("mysql://localhost/database?pool_recycle=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"pool_recycle": 20}


def test_asyncmy_pool_size():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_asyncmy_unix_socket():
    backend = AsyncMyBackend(
        "mysql+asyncmy://username:password@/testsuite?unix_socket=/tmp/mysqld/mysqld.sock"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"unix_socket": "/tmp/mysqld/mysqld.sock"}


def test_asyncmy_explicit_pool_size():
    backend = AsyncMyBackend("mysql://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_asyncmy_ssl():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_asyncmy_explicit_ssl():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_asyncmy_pool_recycle():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database?pool_recycle=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"pool_recycle": 20}


def test_mssql_pool_size():
    backend = MySQLBackend("mssql+pyodbc://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mssql_explicit_pool_size():
    backend = MySQLBackend("mssql+pyodbc://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mssql_ssl():
    backend = MySQLBackend("mssql+pyodbc://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_mssql_explicit_ssl():
    backend = MySQLBackend("mssql+pyodbc://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_mssql_pool_recycle():
    backend = MySQLBackend("mssql+pyodbc://localhost/database?pool_recycle=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"pool_recycle": 20}
