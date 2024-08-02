import inspect
import typing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from importlib import import_module
from types import ModuleType

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


def get_pool_for(pool: typing.Literal["thread", "process"]) -> typing.Any:
    assert pool in {"thread", "process"}, "invalid option"
    if pool == "thread":
        return ThreadPoolExecutor(max_workers=1, thread_name_prefix="dapi2")
    else:
        return ProcessPoolExecutor(max_workers=1)


class DBAPI2_dialect(DefaultDialect):
    """
    Takes a (a)dbapi object and wraps the functions. Generalized
    """

    driver = "dbapi2"

    is_async = True
    supports_statement_cache = True

    def __init__(
        self,
        *,
        dialect_overwrites: typing.Optional[typing.Dict[str, typing.Any]] = None,
        json_serializer: typing.Any = None,
        json_deserializer: typing.Any = None,
        **kwargs: typing.Any,
    ):
        super().__init__(**kwargs)
        if dialect_overwrites:
            for k, v in dialect_overwrites.items():
                setattr(self, k, v)

    def create_connect_args(self, url: "URL") -> "ConnectArgsType":
        dbapi2_dsn_driver: typing.Optional[str] = url.query.get("dbapi2_dsn_driver")  # type: ignore
        driver_args: typing.Any = url.query.get("dbapi2_driver_args")
        if driver_args:
            driver_args = orjson.loads(driver_args)
        dsn: str = url.difference_update_query(
            ("dbapi2_dsn_driver", "dbapi2_driver_args")
        ).render_as_string(hide_password=False)
        if dbapi2_dsn_driver:
            dsn = dsn.replace("dbapi2:", dbapi2_dsn_driver, 1)
        else:
            dsn = dsn.replace("dbapi2://", "", 1)

        return (dsn,), driver_args or {}

    def connect(self, *arg: typing.Any, **kw: typing.Any) -> AsyncAdapt_dbapi_connection:  # type: ignore
        creator_fn = kw.pop("async_creator_fn", self.loaded_dbapi.connect)
        return AsyncAdapt_dbapi_connection(  # type: ignore
            self,
            await_only(creator_fn(*arg, **kw)),
        )

    @classmethod
    def get_pool_class(cls, url: "URL") -> typing.Any:
        return AsyncAdaptedQueuePool

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
    def import_dbapi(  # type: ignore
        cls,
        dbapi_path: str,
        dbapi_pool: typing.Literal["thread", "process"] = "thread",
        dbapi_force_async_wrapper: typing.Optional[bool] = None,
    ) -> ModuleType:
        attr = ""
        if ":" in dbapi_path:
            dbapi_path, attr = dbapi_path.rsplit(":", 1)

        dbapi_namespace = import_module(dbapi_path)
        if attr:
            dbapi_namespace = getattr(dbapi_namespace, attr)
        if dbapi_force_async_wrapper is None:
            # is async -> no need for the wrapper
            if not inspect.iscoroutinefunction(dbapi_namespace.connect):
                dbapi_namespace = AsyncWrapper(
                    dbapi_namespace,
                    get_pool_for(dbapi_pool),
                    exclude_attrs={"connect": {"cursor": True}},
                )  # type: ignore
        elif dbapi_force_async_wrapper:
            dbapi_namespace = AsyncWrapper(dbapi_namespace, get_pool_for(dbapi_pool))  # type: ignore
        return dbapi_namespace


dialect = DBAPI2_dialect
