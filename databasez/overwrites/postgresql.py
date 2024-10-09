from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

from databasez.sqlalchemy import SQLAlchemyConnection, SQLAlchemyDatabase

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement

    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: dict[str, Any],
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pscopg2"}:
            database_url_new = database_url_new.replace(driver="psycopg")
        return database_url_new, options


class Connection(SQLAlchemyConnection):
    async def batched_iterate(
        self, query: ClauseElement, batch_size: int | None = None
    ) -> AsyncGenerator[Any, None]:
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
