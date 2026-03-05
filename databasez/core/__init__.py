"""Core module for databasez providing the main database abstractions.

This package contains the primary public API classes:

- :class:`Database` — The main entry point for connecting to and querying databases.
- :class:`DatabaseURL` — URL parser and builder for database connection strings.
- :class:`Connection` — Represents an active database connection with query methods.
- :class:`Transaction` — Manages database transactions with commit/rollback semantics.
"""

from .connection import Connection
from .database import Database, init
from .databaseurl import DatabaseURL
from .transaction import Transaction

__all__ = ["Connection", "Database", "init", "DatabaseURL", "Transaction"]
