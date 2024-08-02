import typing
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module

import orjson
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql import text
from sqlalchemy.util.concurrency import await_only
from sqlalchemy_utils.functions.orm import quote

from databasez.utils import AsyncWrapper

if typing.TYPE_CHECKING:
    from sqlalchemy import URL
    from sqlalchemy.base import Connection
    from sqlalchemy.engine.interfaces import ConnectArgsType


class AsyncAdapt_adbapi2_connection(AsyncAdapt_dbapi_connection):
    pass


class JDBC_dialect(DefaultDialect):
    """
    Takes a (a)dbapi object and wraps the functions.
    """

    driver = "jdbc"
    supports_statement_cache = True

    is_async = True

    def __init__(
        self,
        json_serializer: typing.Any = None,
        json_deserializer: typing.Any = None,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(**kwargs)

    def create_connect_args(self, url: "URL") -> "ConnectArgsType":
        jdbc_dsn_driver: str = typing.cast(str, url.query["jdbc_dsn_driver"])
        jdbc_driver: typing.Optional[str] = typing.cast(
            typing.Optional[str], url.query.get("jdbc_driver")
        )
        driver_args: typing.Any = url.query.get("jdbc_driver_args")
        if driver_args:
            driver_args = orjson.loads(driver_args)
        dsn: str = url.difference_update_query(
            ("jdbc_driver", "jdbc_driver_args", "jdbc_dsn_driver")
        ).render_as_string(hide_password=False)
        dsn = dsn.replace("jdbc://", f"jdbc:{jdbc_dsn_driver}:", 1)

        return (dsn,), {"driver_args": driver_args, "driver": jdbc_driver}

    def connect(self, *arg: typing.Any, **kw: typing.Any) -> AsyncAdapt_adbapi2_connection:
        creator_fn = AsyncWrapper(
            self.loaded_dbapi,
            pool=ThreadPoolExecutor(1, thread_name_prefix="jpype"),
            stringify_exceptions=True,
            exclude_attrs={"connect": {"cursor": True}},
        ).connect

        creator_fn = kw.pop("async_creator_fn", creator_fn)

        return AsyncAdapt_adbapi2_connection(  # type: ignore
            self,
            await_only(creator_fn(*arg, **kw)),
        )

    def has_table(
        self,
        connection: "Connection",
        table_name: str,
        schema: typing.Optional[str] = None,
        **kw: typing.Any,
    ) -> bool:
        stmt = text(f"select * from '{quote(connection, table_name)}' LIMIT 1")
        try:
            connection.execute(stmt)
            return True
        except Exception:
            return False

    def get_isolation_level(self, dbapi_connection: typing.Any) -> typing.Any:
        return None

    @classmethod
    def get_pool_class(cls, url: "URL") -> typing.Any:
        return AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(
        cls,
    ) -> typing.Any:
        return import_module("jpype.dbapi2")


dialect = JDBC_dialect
