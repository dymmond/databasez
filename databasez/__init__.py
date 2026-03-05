"""Databasez - Async database support for Python.

Databasez provides a lightweight async database abstraction layer built on top
of SQLAlchemy. It supports multiple database backends (PostgreSQL, MySQL,
SQLite, MSSQL) through a unified async interface, with features like connection
pooling, transactions, force rollback testing, and multi-loop isolation.

Example:
    >>> from databasez import Database, DatabaseURL
    >>> async with Database("sqlite:///example.db") as database:
    ...     await database.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)")
"""

from databasez.core import Database, DatabaseURL

__version__ = "0.12.0"

__all__ = ["Database", "DatabaseURL"]
