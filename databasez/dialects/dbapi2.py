from __future__ import annotations

import inspect
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal

import orjson
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql import text
from sqlalchemy.util.concurrency import await_only

from databasez.utils import AsyncWrapper

if TYPE_CHECKING:
    from sqlalchemy import URL
    from sqlalchemy.base import Connection
    from sqlalchemy.engine.interfaces import ConnectArgsType


def get_pool_for(pool: Literal["thread", "process"]) -> Any:
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
        dialect_overwrites: dict[str, Any] | None = None,
        json_serializer: Any = None,
        json_deserializer: Any = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if dialect_overwrites:
            for k, v in dialect_overwrites.items():
                setattr(self, k, v)

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        dbapi_dsn_driver: str | None = url.query.get("dbapi_dsn_driver")  # type: ignore
        driver_args: Any = url.query.get("dbapi_driver_args")
        dbapi_pool: str | None = url.query.get("dbapi_pool")  # type: ignore
        dbapi_force_async_wrapper: str = url.query.get("dbapi_force_async_wrapper")  # type: ignore
        if driver_args:
            driver_args = orjson.loads(driver_args)
        dsn: str = url.difference_update_query(
            ("dbapi_dsn_driver", "dbapi_driver_args")
        ).render_as_string(hide_password=False)
        dsn = (
            dsn.replace("dbapi2:", dbapi_dsn_driver, 1)
            if dbapi_dsn_driver
            else dsn.replace("dbapi2://", "", 1)
        )
        kwargs_passed = {
            "driver_args": driver_args,
            "dbapi_pool": dbapi_pool,
            "dbapi_force_async_wrapper": (
                True
                if dbapi_force_async_wrapper == "true"
                else (False if dbapi_force_async_wrapper == "false" else None)
            ),
        }

        return (dsn,), {k: v for k, v in kwargs_passed.items() if v is not None}

    def connect(
        self,
        *arg: Any,
        dbapi_pool: Literal["thread", "process"] = "thread",
        dbapi_force_async_wrapper: bool | None = None,
        driver_args: Any = None,
        **kw: Any,
    ) -> AsyncAdapt_dbapi_connection:
        dbapi_namespace = self.loaded_dbapi
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
        creator_fn = kw.pop("async_creator_fn", dbapi_namespace.connect)
        return AsyncAdapt_dbapi_connection(  # type: ignore
            self,
            await_only(creator_fn(*arg, **(driver_args or {}))),
        )

    @classmethod
    def get_pool_class(cls, url: URL) -> Any:
        return AsyncAdaptedQueuePool

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> bool:
        quoted = self.identifier_preparer.quote(table_name)
        stmt = text(f"select 1 from '{quoted}' LIMIT 1")
        try:
            connection.execute(stmt)
            return True
        except Exception:
            return False

    def get_isolation_level(self, dbapi_connection: Any) -> Any:
        return None

    @classmethod
    def import_dbapi(  # type: ignore
        cls,
        dbapi_path: str,
    ) -> ModuleType:
        attr = ""
        if ":" in dbapi_path:
            dbapi_path, attr = dbapi_path.rsplit(":", 1)

        dbapi_namespace = import_module(dbapi_path)
        if attr:
            dbapi_namespace = getattr(dbapi_namespace, attr)
        return dbapi_namespace


dialect = DBAPI2_dialect
