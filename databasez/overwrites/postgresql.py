from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyConnection, SQLAlchemyDatabase

if TYPE_CHECKING:
    try:
        from sqlalchemy.sql import ClauseElement
    except Exception:
        ClauseElement = Any

    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    """Database backend for PostgreSQL.

    Automatically swaps the synchronous ``psycopg2`` driver for the async
    ``psycopg`` driver during option extraction.
    """

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: dict[str, Any],
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        """Extract and normalise PostgreSQL-specific connection options.

        Replaces the ``psycopg2`` (or unset) driver with ``psycopg``.

        Args:
            database_url: The original database URL.
            **options: Arbitrary connection options.

        Returns:
            tuple[DatabaseURL, dict[str, Any]]: The rewritten URL and
                remaining options dictionary.
        """
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "psycopg2"}:
            database_url_new = database_url_new.replace(driver="psycopg")
        return database_url_new, options


class Connection(SQLAlchemyConnection):
    """Connection backend for PostgreSQL.

    Overrides streaming iteration methods to ensure they always execute
    inside a transaction, as required by the PostgreSQL protocol for
    server-side cursors.
    """

    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
        """Iterate over query results in batches.

        If a transaction is already active the results are yielded
        directly.  Otherwise a new transaction is opened for the
        duration of the iteration (PostgreSQL requires an active
        transaction for server-side cursors).

        Args:
            query: The SQLAlchemy clause element to execute.
            batch_size: Number of rows per batch.  ``None`` uses the
                backend default.

        Yields:
            Any: A batch of rows from the result set.
        """
        # postgres needs a transaction for iterate/batched_iterate
        if self.in_transaction():
            owner = self.owner
            assert owner is not None
            async for batch in super().batched_iterate(query, batch_size):
                yield batch
        else:
            owner = self.owner
            assert owner is not None
            async with owner.transaction():
                async for batch in super().batched_iterate(query, batch_size):
                    yield batch

    async def iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
        """Iterate over query results one row at a time.

        If a transaction is already active the rows are yielded
        directly.  Otherwise a new transaction is opened for the
        duration of the iteration (PostgreSQL requires an active
        transaction for server-side cursors).

        Args:
            query: The SQLAlchemy clause element to execute.
            batch_size: Internal fetch size hint.  ``None`` uses the
                backend default.

        Yields:
            Any: A single row from the result set.
        """
        # postgres needs a transaction for iterate
        if self.in_transaction():
            owner = self.owner
            assert owner is not None
            async for row in super().iterate(query, batch_size):
                yield row
        else:
            owner = self.owner
            assert owner is not None
            async with owner.transaction():
                async for row in super().iterate(query, batch_size):
                    yield row
