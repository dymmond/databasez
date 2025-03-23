from __future__ import annotations

import contextlib
import re
from collections.abc import Collection, Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

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
    TableKey,
)
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql import sqltypes, text
from sqlalchemy.util.concurrency import await_only

from databasez.utils import AsyncWrapper

if TYPE_CHECKING:
    from jpype.dbapi2 import Connection as JDBCConnection
    from sqlalchemy import URL
    from sqlalchemy.base import Connection
    from sqlalchemy.engine.interfaces import ConnectArgsType


class AsyncAdapt_adbapi2_connection(AsyncAdapt_dbapi_connection):
    pass


def unpack_to_jdbc_connection(connection: Connection) -> JDBCConnection:
    return connection.connection.dbapi_connection.driver_connection.connection


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
        transform_reflected_names: Literal["none", "upper", "lower"] = "none",
        use_code_datatype: bool = False,
        **kwargs: Any,
    ) -> None:
        self.transform_reflected_names = transform_reflected_names
        self.use_code_datatype = use_code_datatype
        super().__init__(**kwargs)
        # dbapi to query is available

    def transform_refl_name(self, name: Any) -> str:
        name = str(name)
        if self.transform_reflected_names == "lower":
            return cast(str, name.lower())
        elif self.transform_reflected_names == "upper":
            return cast(str, name.upper())
        return cast(str, name)

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
        quoted = self.identifier_preparer.quote(table_name)
        stmt = text(f"SELECT 1 from '{quoted}' LIMIT 1")
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

    @staticmethod
    @lru_cache(512)
    def _escape_jdbc_name(inp: str, escape: Any) -> str:
        escape = str(escape) if escape else "\\"
        re_escaped = re.escape(escape)
        result_escape = escape
        if result_escape == "\\":
            result_escape = "\\\\"
        return re.sub(f"({re_escaped}|_|%)", f"{result_escape}\\1", str(inp))

    @reflection.cache
    def get_schema_names(self, connection: Connection, **kw: Any) -> list[str]:
        jdbc_connection = connection.connection.dbapi_connection.connection
        metadata = jdbc_connection.getMetaData()
        table_set = metadata.getSchemas()
        names: list[str] = []
        try:
            while table_set.next():
                names.append(self.transform_refl_name(table_set.getString("TABLE_SCHEM")))
        finally:
            table_set.close()
        return names

    @reflection.cache
    def get_table_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = unpack_to_jdbc_connection(connection)

        # .driver_connection.connection
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["TABLE"]
        )
        names: list[str] = []
        try:
            while table_set.next():
                names.append(self.transform_refl_name(table_set.getString("TABLE_NAME")))
        finally:
            table_set.close()
        return names

    @reflection.cache
    def get_temp_table_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            None,
            ["GLOBAL TEMPORARY", "LOCAL TEMPORARY"],
        )
        names: list[str] = []
        try:
            while table_set.next():
                names.append(self.transform_refl_name(table_set.getString("TABLE_NAME")))
        finally:
            table_set.close()
        return names

    @reflection.cache
    def get_temp_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["VIEW"]
        )
        names: list[str] = []
        try:
            while table_set.next():
                names.append(self.transform_refl_name(table_set.getString("TABLE_NAME")))
        finally:
            table_set.close()
        return names

    @reflection.cache
    def get_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        table_set = metadata.getTables(
            None, self._escape_jdbc_name(schema, escape) if schema else "", None, ["VIEW"]
        )
        names: list[str] = []
        try:
            while table_set.next():
                names.append(self.transform_refl_name(table_set.getString("TABLE_NAME")))
        finally:
            table_set.close()
        return names

    @reflection.cache
    def get_view_definition(
        self, connection: Connection, view_name: str, schema: str | None = None, **kw: Any
    ) -> str:
        raise NotImplementedError()

    def get_multi_columns(
        self,
        connection: Connection,
        *,
        schema: None | str = None,
        filter_names: None | Collection[str] = None,
        **kw: Any,
    ) -> Iterable[tuple[TableKey, list[ReflectedColumn]]]:
        # this requires the jpype.imports import in overwrites
        from java.sql import SQLException
        from jpype import dbapi2

        columns_dict: dict[TableKey, list[ReflectedColumn]] = {}
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        jdbc_columns = metadata.getColumns(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            "%",
            None,
        )

        try:
            while jdbc_columns.next():
                if self.use_code_datatype:
                    code_datatype: int = jdbc_columns.getInt("DATA_TYPE")
                    string_datatype = str(dbapi2._registry.get(code_datatype))
                else:
                    string_datatype = str(jdbc_columns.getString("TYPE_NAME")).split("(", 1)[0]
                sqlalchemy_datatype_class = getattr(sqltypes, string_datatype.upper())
                # length or precision
                datatype_size: int | None = jdbc_columns.getInt("COLUMN_SIZE")
                # decimal digits
                datatype_digits: int | None = jdbc_columns.getInt("DECIMAL_DIGITS")

                type_name = sqlalchemy_datatype_class().as_generic().__class__.__name__

                field_params = {}
                if type_name in {"String", "LargeBinary"}:
                    field_params["length"] = datatype_size
                elif type_name == "Numeric":
                    field_params["precision"] = datatype_size
                    field_params["scale"] = datatype_digits
                sqlalchemy_datatype = sqlalchemy_datatype_class(**field_params)
                schema = jdbc_columns.getString("TABLE_SCHEM")
                default_str = jdbc_columns.getString("COLUMN_DEF")
                # some implementations return incorrectly just one char
                nullable_val = str(jdbc_columns.getString("IS_NULLABLE")).upper()
                reflected_col = ReflectedColumn(
                    name=str(jdbc_columns.getString("COLUMN_NAME")),
                    type=sqlalchemy_datatype,
                    default=None if default_str is None else str(default_str),
                    nullable=nullable_val.startswith("Y"),
                )
                # old jdbc libraries raise on this reflection
                with contextlib.suppress(SQLException):
                    autoincrement_val = str(jdbc_columns.getString("IS_AUTOINCREMENT")).upper()
                    # empty string for unknown
                    if autoincrement_val:
                        reflected_col["autoincrement"] = autoincrement_val.startswith("Y")

                columns_dict.setdefault(
                    (
                        None if schema is None else str(schema),
                        str(jdbc_columns.getString("TABLE_NAME")),
                    ),
                    [],
                ).append(reflected_col)
        finally:
            jdbc_columns.close()
        return columns_dict.items()

    @reflection.cache
    def get_pk_constraint(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw: Any
    ) -> ReflectedPrimaryKeyConstraint:
        constraint_name: str | None = None
        pkeys: list[str] = []
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        jdbc_pkeys = metadata.getPrimaryKeys(
            None,
            self._escape_jdbc_name(schema, escape) if schema else "",
            self._escape_jdbc_name(table_name, escape),
        )
        is_first: bool = True
        try:
            while jdbc_pkeys.next():
                if is_first:
                    constraint_name = jdbc_pkeys.getString("PK_NAME")
                pkeys.append(jdbc_pkeys.getString("COLUMN_NAME"))

                is_first = False
        finally:
            jdbc_pkeys.close()
        if pkeys:
            return {"constrained_columns": pkeys, "name": constraint_name}
        else:
            return reflection.ReflectionDefaults.pk_constraint()

    @reflection.cache
    def get_foreign_keys(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedForeignKeyConstraint]:
        # this requires the jpype.imports import in overwrites
        from java.sql import SQLException

        # name, target schema, target table, update, delete, from, to
        fkeys: list[tuple[str | None, str, str, int, int, list[str], list[str]]] = []
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        try:
            jdbc_fkeys = metadata.getImportedKeys(
                None,
                self._escape_jdbc_name(schema, escape) if schema else "",
                self._escape_jdbc_name(table_name, escape),
            )
        except SQLException:
            # some jdbc drivers have not implemented this
            return reflection.ReflectionDefaults.foreign_keys()
        last_seq: int = 0
        try:
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
        finally:
            jdbc_fkeys.close()
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
        # this requires the jpype.imports import in overwrites
        from java.sql import SQLException

        # name, unique, columns, expressions, sortings
        indexes: list[tuple[str | None, bool, list[str | None], list[str], list[str | None]]] = []
        jdbc_connection = unpack_to_jdbc_connection(connection)
        metadata = jdbc_connection.getMetaData()
        escape = metadata.getSearchStringEscape()
        try:
            jdbc_indexes = metadata.getIndexInfo(
                None,
                self._escape_jdbc_name(schema, escape) if schema else "",
                self._escape_jdbc_name(table_name, escape),
                unique,
                False,
            )
        except SQLException:
            # some jdbc drivers have not implemented this
            return reflection.ReflectionDefaults.indexes()
        last_seq: int = 0
        try:
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
        finally:
            jdbc_indexes.close()
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
