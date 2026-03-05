from __future__ import annotations

import inspect
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal

import orjson
from sqlalchemy import text
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import await_only

from databasez.utils import AsyncWrapper

if TYPE_CHECKING:
    from sqlalchemy.engine import URL, Connection

    try:
        from sqlalchemy.engine.interfaces import ConnectArgsType
    except Exception:
        ConnectArgsType = Any


def get_pool_for(pool: Literal["thread", "process"]) -> Any:
    """Create an executor pool for wrapping synchronous DBAPI calls.

    Args:
        pool: The type of executor to create.  Must be ``"thread"``
            or ``"process"``.

    Returns:
        Any: A :class:`~concurrent.futures.ThreadPoolExecutor` or
            :class:`~concurrent.futures.ProcessPoolExecutor` with a
            single worker.

    Raises:
        AssertionError: If *pool* is not ``"thread"`` or ``"process"``.
    """
    assert pool in {"thread", "process"}, "invalid option"
    if pool == "thread":
        return ThreadPoolExecutor(max_workers=1, thread_name_prefix="dapi2")
    else:
        return ProcessPoolExecutor(max_workers=1)


class DBAPI2_dialect(DefaultDialect):
    """Async SQLAlchemy dialect for generic DBAPI 2.0 drivers.

    Wraps any PEP 249-compliant driver module and exposes it via
    SQLAlchemy's async connection infrastructure.  Synchronous drivers
    are wrapped in an :class:`~databasez.utils.AsyncWrapper` backed by
    a configurable executor pool; natively async drivers are used
    directly.

    Attributes:
        driver: The dialect driver name (``"dbapi2"``).
        is_async: Always ``True``.
        supports_statement_cache: Always ``True``.
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
        """Initialise the DBAPI2 dialect.

        Args:
            dialect_overwrites: Optional mapping of attribute names to
                values that will be set on the dialect instance,
                allowing runtime customisation of dialect behaviour.
            json_serializer: JSON serializer callable (unused by the
                dialect itself but accepted for interface compatibility).
            json_deserializer: JSON deserializer callable (unused by the
                dialect itself but accepted for interface compatibility).
            **kwargs: Forwarded to
                :class:`~sqlalchemy.engine.default.DefaultDialect`.
        """
        super().__init__(**kwargs)
        if dialect_overwrites:
            for k, v in dialect_overwrites.items():
                setattr(self, k, v)

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """Build positional and keyword arguments for ``connect()``.

        Parses DBAPI2-specific query parameters from *url*, constructs
        the DSN string, and returns the argument tuple expected by
        SQLAlchemy's engine machinery.

        Args:
            url: The SQLAlchemy engine URL.

        Returns:
            ConnectArgsType: A 2-tuple of ``(args, kwargs)`` to be
                passed to :meth:`connect`.
        """
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

    def connect(  # type: ignore
        self,
        *arg: Any,
        dbapi_pool: Literal["thread", "process"] = "thread",
        dbapi_force_async_wrapper: bool | None = None,
        driver_args: Any = None,
        **kw: Any,
    ) -> AsyncAdapt_dbapi_connection:
        """Establish a DBAPI connection, optionally wrapping it for async.

        If the loaded DBAPI module's ``connect`` function is synchronous
        and *dbapi_force_async_wrapper* is not explicitly ``False``, the
        module is wrapped with :class:`~databasez.utils.AsyncWrapper`.

        Args:
            *arg: Positional arguments forwarded to the driver's
                ``connect()``.
            dbapi_pool: Executor pool type used by the async wrapper.
            dbapi_force_async_wrapper: Explicit override.  ``True``
                always wraps, ``False`` never wraps, ``None``
                auto-detects.
            driver_args: Extra keyword arguments merged into the
                ``connect()`` call.
            **kw: Additional keyword arguments.  ``async_creator_fn``
                overrides the default ``connect`` callable.

        Returns:
            AsyncAdapt_dbapi_connection: An async-adapted DBAPI
                connection wrapper.
        """
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
        return AsyncAdapt_dbapi_connection(
            self,
            await_only(creator_fn(*arg, **(driver_args or {}))),
        )

    @classmethod
    def get_pool_class(cls, url: URL) -> Any:
        """Return the connection pool class to use.

        Args:
            url: The SQLAlchemy engine URL.

        Returns:
            Any: :class:`~sqlalchemy.pool.AsyncAdaptedQueuePool`.
        """
        return AsyncAdaptedQueuePool

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> bool:
        """Check whether a table exists in the database.

        Executes a simple ``SELECT 1`` probe against the table.

        Args:
            connection: The active SQLAlchemy connection.
            table_name: Name of the table to check.
            schema: Optional schema name (unused for DBAPI2).
            **kw: Additional keyword arguments (ignored).

        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise.
        """
        quoted = self.identifier_preparer.quote(table_name)
        stmt = text(f"select 1 from '{quoted}' LIMIT 1")
        try:
            connection.execute(stmt)
            return True
        except Exception:
            return False

    def get_isolation_level(self, dbapi_connection: Any) -> Any:
        """Return the current isolation level of a DBAPI connection.

        Args:
            dbapi_connection: The raw DBAPI connection.

        Returns:
            Any: Always ``None`` (isolation level tracking is not
                supported by the generic DBAPI2 dialect).
        """
        return None

    @classmethod
    def import_dbapi(  # type: ignore
        cls,
        dbapi_path: str,
    ) -> ModuleType:
        """Dynamically import a DBAPI module by dotted path.

        The *dbapi_path* may optionally contain a colon-separated
        attribute suffix (e.g. ``"some.module:SubNamespace"``) to
        select a nested namespace within the imported module.

        Args:
            dbapi_path: Dotted Python import path, optionally followed
                by ``":attribute"``.

        Returns:
            ModuleType: The imported DBAPI module (or attribute
                thereof).
        """
        attr = ""
        if ":" in dbapi_path:
            dbapi_path, attr = dbapi_path.rsplit(":", 1)

        dbapi_namespace = import_module(dbapi_path)
        if attr:
            dbapi_namespace = getattr(dbapi_namespace, attr)
        return dbapi_namespace


dialect = DBAPI2_dialect
