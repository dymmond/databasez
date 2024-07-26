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


def get_pool_for(pool: typing.Literal["thread", "process"]):
    assert pool in {"thread", "process"}, "invalid option"
    if pool == "thread":
        return ThreadPoolExecutor(max_workers=1, thread_name_prefix="adapi2")
    else:
        return ProcessPoolExecutor(max_workers=1)


class ADBAPI2_dialect(DefaultDialect):
    """
    Takes a (a)dbapi object and wraps the functions.
    """

    driver = "adbapi2"

    is_async = True

    def __init__(
        self,
        *,
        dialect_overwrites: typing.Optional[typing.Dict[str, typing.Any]] = None,
        **kwargs: typing.Any,
    ):
        super.__init__(**kwargs)
        if dialect_overwrites:
            for k, v in dialect_overwrites.items():
                setattr(self, k, v)

    def connect(self, *arg, **kw):
        creator_fn = kw.pop("async_creator_fn", self.aiomysql.connect)

        return AsyncAdapt_adbapi2_connection(
            self,
            await_(creator_fn(*arg, **kw)),
        )

    @classmethod
    def import_dbapi(
        cls,
        dbapi_path: str,
        dbapi_pool: typing.Literal["thread", "process"],
        dbapi_force_async_wrapper: typing.Optional[bool] = None,
    ) -> typing.Any:
        attr = ""
        if ":" in dbapi_path:
            dbapi_path, attr = dbapi_path.rsplit(":", 1)

        dbapi_namespace = __import__(dbapi_path)
        if attr:
            dbapi_namespace = getattr(dbapi_namespace, attr)
        if dbapi_force_async_wrapper is None:
            # is async -> no need for the wrapper
            if not inspect.iscoroutinefunction(dbapi_namespace.connect):
                dbapi_namespace = AsyncWrapper(dbapi_namespace, get_pool_for(dbapi_pool))
        elif dbapi_force_async_wrapper:
            dbapi_namespace = AsyncWrapper(dbapi_namespace, get_pool_for(dbapi_pool))
        return dbapi_namespace


dialect = ADBAPI2_dialect
