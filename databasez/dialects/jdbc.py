import inspect
import typing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.util.concurrency import await_

from databasez.utils import AsyncWrapper


class AsyncAdapt_adbapi2_connection(AsyncAdapt_dbapi_connection):
    pass


class JDBC_dialect(DefaultDialect):
    """
    Takes a (a)dbapi object and wraps the functions.
    """

    driver = "jdbc"

    is_async = True

    def connect(self, *arg, **kw):
        creator_fn = kw.pop("async_creator_fn", self.aiomysql.connect)

        return AsyncAdapt_adbapi2_connection(
            self,
            await_(creator_fn(*arg, **kw)),
        )

    @classmethod
    def import_dbapi(
        cls,
    ) -> typing.Any:
        return AsyncWrapper(
            __import__("jpype.dbapi2"), pool=ThreadPoolExecutor(1, thread_name_prefix="jpype")
        )


dialect = JDBC_dialect
