import typing

from databasez.sqlalchemy import SQLAlchemyConnection, SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement

    from databasez.core.databaseurl import DatabaseURL


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        if database_url_new.driver in {None, "pscopg2"}:
            database_url_new = database_url_new.replace(driver="psycopg")
        return database_url_new, options


class Connection(SQLAlchemyConnection):
    async def execute(self, stmt: typing.Any) -> int:
        """
        Executes statement and returns the last row id (query) or the row count of updates.

        Warning: can return -1 (e.g. psycopg) in case the result is unknown

        """
        with await self.execute_raw(stmt) as result:
            try:
                return typing.cast(int, result.lastrowid)
            except AttributeError:
                if result.is_insert and result.returned_defaults:
                    return typing.cast(int, result.returned_defaults[0])
                return typing.cast(int, result.rowcount)

    async def batched_iterate(
        self, query: "ClauseElement", batch_size: typing.Optional[int] = None
    ) -> typing.AsyncGenerator[typing.Any, None]:
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
