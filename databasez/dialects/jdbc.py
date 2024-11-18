from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from importlib import import_module
from typing import TYPE_CHECKING, Any, Optional, cast

import orjson
from sqlalchemy import exc
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
)
from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import (
    ReflectedCheckConstraint,
    ReflectedColumn,
    ReflectedForeignKeyConstraint,
    ReflectedIndex,
    ReflectedPrimaryKeyConstraint,
    ReflectedUniqueConstraint,
)
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql import sqltypes, text
from sqlalchemy.util.concurrency import await_only
from sqlalchemy_utils.functions.orm import quote

from databasez.utils import AsyncWrapper

if TYPE_CHECKING:
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
        json_serializer: Any = None,
        json_deserializer: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # dbapi to query is available

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        jdbc_dsn_driver: str = cast(str, url.query["jdbc_dsn_driver"])
        jdbc_driver: str | None = cast(Optional[str], url.query.get("jdbc_driver"))
        driver_args: Any = url.query.get("jdbc_driver_args")
        if driver_args:
            driver_args = orjson.loads(driver_args)
        dsn: str = url.difference_update_query(
            ("jdbc_driver", "jdbc_driver_args", "jdbc_dsn_driver")
        ).render_as_string(hide_password=False)
        dsn = dsn.replace("jdbc://", f"jdbc:{jdbc_dsn_driver}:", 1)

        return (dsn,), {"driver_args": driver_args, "driver": jdbc_driver}

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_adbapi2_connection:
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

    @reflection.cache
    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> bool:
        stmt = text(f"select * from '{quote(connection, table_name)}' LIMIT 1")
        try:
            connection.execute(stmt)
            return True
        except Exception:
            return False

    def get_isolation_level(self, dbapi_connection: Any) -> Any:
        return None

    @classmethod
    def get_pool_class(cls, url: URL) -> Any:
        return AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(
        cls,
    ) -> Any:
        return import_module("jpype.dbapi2")

    @lru_cache(512)
    @staticmethod
    def _escape_jdbc_name(inp: str, escape: str) -> str:
        re_escaped = re.escape(escape)
        return re.sub(f"({re_escaped}|_|%)", inp, f"{escape}\\1")

    @reflection.cache
    def get_schema_names(self, connection: Connection, **kw: Any) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        table_set = metadata.getSchemas()
        names: list[str] = []
        while table_set.next():
            names.append(table_set.getString("TABLE_SCHEM"))
        return names

    @reflection.cache
    def get_table_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["TABLE"]
        )
        names: list[str] = []
        while table_set.next():
            names.append(table_set.getString("TABLE_NAME"))
        return names

    @reflection.cache
    def get_temp_table_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            None,
            ["GLOBAL TEMPORARY", "LOCAL TEMPORARY"],
        )
        names: list[str] = []
        while table_set.next():
            names.append(table_set.getString("TABLE_NAME"))
        return names

    @reflection.cache
    def get_temp_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["VIEW"]
        )
        names: list[str] = []
        while table_set.next():
            names.append(table_set.getString("TABLE_NAME"))
        return names

    @reflection.cache
    def get_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["VIEW"]
        )
        names: list[str] = []
        while table_set.next():
            names.append(table_set.getString("TABLE_NAME"))
        return names

    @reflection.cache
    def get_view_definition(
        self, connection: Connection, view_name: str, schema: str | None = None, **kw: Any
    ) -> str:
        raise NotImplementedError()

    @reflection.cache
    def get_columns(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedColumn]:
        from jpype1 import dbapi2

        columns: list[ReflectedColumn] = []

        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        jdbc_columns = metadata.getColumns(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            self._escape_jdbc_name(table_name, escape),
            None,
        )

        while jdbc_columns.next():
            code_datatype: int = jdbc_columns.getInteger("DATA_TYPE")
            jpype_datatype = dbapi2._registry.get(code_datatype)
            sqlalchemy_datatype_class = getattr(sqltypes, jpype_datatype.name.upper())
            # length or precision
            datatype_size: int | None = jdbc_columns.getInteger("COLUMN_SIZE")
            # decimal digits
            datatype_digits: int | None = jdbc_columns.getInteger("DECIMAL_DIGITS")

            type_name = sqlalchemy_datatype_class.as_generic().__name__

            field_params = {}
            if type_name in {"String", "LargeBinary"}:
                field_params["length"] = datatype_size
            elif type_name == "Numeric":
                field_params["precision"] = datatype_size
                field_params["scale"] = datatype_digits
            sqlalchemy_datatype = sqlalchemy_datatype_class(**field_params)

            columns.append(
                ReflectedColumn(
                    name=jdbc_columns.getString("COLUMN_NAME"),
                    type=sqlalchemy_datatype,
                    default=jdbc_columns.getString("COLUMN_DEF"),
                    nullable=jdbc_columns.getString("IS_NULLABLE").lower() != "false",
                    autoincrement=jdbc_columns.getString("IS_AUTOINCREMENT").lower() == "true",
                )
            )
        if columns:
            return columns
        elif not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(f"{schema}.{table_name}" if schema else table_name)
        else:
            return reflection.ReflectionDefaults.columns()

    @reflection.cache
    def get_pk_constraint(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw: Any
    ) -> ReflectedPrimaryKeyConstraint:
        constraint_name: str | None = None
        pkeys: list[str] = []
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        jdbc_pkeys = metadata.getPrimaryKeys(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            self._escape_jdbc_name(table_name, escape),
        )
        is_first: bool = True
        while jdbc_pkeys.next():
            if is_first:
                constraint_name = jdbc_pkeys.getString("PK_NAME")
            pkeys.append(jdbc_pkeys.getString("COLUMN_NAME"))

            is_first = False
        if pkeys:
            return {"constrained_columns": pkeys, "name": constraint_name}
        else:
            return reflection.ReflectionDefaults.pk_constraint()

    @reflection.cache
    def get_foreign_keys(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedForeignKeyConstraint]:
        # name, target schema, target table, update, delete, from, to
        fkeys: list[tuple[str | None, str, str, int, int, list[str], list[str]]] = []
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        jdbc_fkeys = metadata.getImportedKeys(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            self._escape_jdbc_name(table_name, escape),
        )
        last_seq: int = 0
        while jdbc_fkeys.next():
            seq = jdbc_fkeys.getShort("KEY_SEQ")
            if seq >= last_seq:
                new_entry: tuple[str | None, str, str, int, int, list[str], list[str]] = (
                    jdbc_fkeys.getString("FK_NAME"),
                    jdbc_fkeys.getString("PKTABLE_SCHEM"),
                    jdbc_fkeys.getString("PKTABLE_NAME"),
                    jdbc_fkeys.getShort("UPDATE_RULE"),
                    jdbc_fkeys.getShort("DELETE_RULE"),
                    [],
                    [],
                )
                fkeys.append(new_entry)
            fkeys[-1][-2].append(jdbc_fkeys.getString("FKCOLUMN_NAME"))
            fkeys[-1][-1].append(jdbc_fkeys.getString("PKCOLUMN_NAME"))
            last_seq = seq
        if fkeys:
            return [
                ReflectedForeignKeyConstraint(
                    name=fkey[0],
                    referred_schema=fkey[1],
                    referred_table=fkey[2],
                    constrained_columns=fkey[-2],
                    referred_columns=fkey[-1],
                    # TODO: add information about DELETE/REMOVE rule
                )
                for fkey in fkeys
            ]
        else:
            return reflection.ReflectionDefaults.foreign_keys()

    @staticmethod
    def get_sort_order(order: str | None) -> tuple[str, ...]:
        if order == "A":
            return ("asc",)
        elif order == "D":
            return ("desc",)
        else:
            return ()

    @reflection.cache
    def get_indexes(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        unique: bool = False,
        **kw: Any,
    ) -> list[ReflectedIndex]:
        # name, unique, columns, expressions, sortings
        indexes: list[tuple[str | None, bool, list[str | None], list[str], list[str | None]]] = []
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetadata()
        escape = metadata.getSearchStringEscape()
        jdbc_indexes = metadata.getIndexInfo(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            self._escape_jdbc_name(table_name, escape),
            unique,
            False,
        )
        last_seq: int = 0
        while jdbc_indexes.next():
            seq = jdbc_indexes.getShort("ORDINAL_POSITION")
            if seq == 0:
                last_seq = seq
                continue
            if seq >= last_seq:
                new_entry: tuple[
                    str | None, bool, list[str | None], list[str], list[str | None]
                ] = (
                    jdbc_indexes.getString("INDEX_NAME"),
                    not jdbc_indexes.getBoolean("NON_UNIQUE"),
                    [],
                    [],
                    [],
                )
                indexes.append(new_entry)
            colname = jdbc_indexes.getString("COLUMN_NAME")
            indexes[-1][-3].append(colname)
            indexes[-1][-2].append(colname)
            indexes[-1][-1].append(jdbc_indexes.getString("ASC_OR_DESC"))
            last_seq = seq
        indexes.sort(key=lambda d: d[0] or "~")  # sort None as last
        if indexes:
            return [
                ReflectedIndex(
                    name=index[0],
                    unique=index[1],
                    column_names=index[-3],
                    expressions=index[-2],
                    column_sorting={
                        col: self.get_sort_order(indexes[-1][num])  # type: ignore
                        for num, col in enumerate(index[-3])
                        if col is not None
                    },
                )
                for index in indexes
            ]
        elif not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(f"{schema}.{table_name}" if schema else table_name)
        else:
            return reflection.ReflectionDefaults.indexes()

    @reflection.cache
    def get_unique_constraints(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[ReflectedUniqueConstraint]:
        data = self.get_indexes(
            connection,
            schema=schema,
            table_name=table_name,
            unique=True,
            **kw,
        )
        return [
            ReflectedUniqueConstraint(
                name=index.name, duplicates_index=index.name, column_names=index.column_names
            )
            for index in data
            if index.unique
        ]

    @reflection.cache
    def get_check_constraints(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[ReflectedCheckConstraint]:
        # FIXME: NOT IMPLEMENTED YET
        # MAYBE implementable via INFORMATION_SCHEMA
        return reflection.ReflectionDefaults.check_constraints()


dialect = JDBC_dialect
