from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

# ensure jpype.dbapi2 is initialized. Prevent race condition.
import jpype.dbapi2  # noqa
import jpype.imports  # noqa
from jpype import addClassPath, isJVMStarted, startJVM

from databasez.sqlalchemy import SQLAlchemyDatabase, SQLAlchemyTransaction

if TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


seen_classpathes: set[str] = set()


class Transaction(SQLAlchemyTransaction):
    def get_default_transaction_isolation_level(
        self, is_root: bool, **extra_options: Any
    ) -> str | None:
        return None


class Database(SQLAlchemyDatabase):
    default_isolation_level = None

    def extract_options(
        self,
        database_url: DatabaseURL,
        **options: Any,
    ) -> tuple[DatabaseURL, dict[str, Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        new_query_options = dict(database_url.options)
        if database_url_new.driver:
            new_query_options["jdbc_dsn_driver"] = database_url_new.driver
        if "classpath" in new_query_options:
            old_classpath = options.pop("classpath", None)
            new_classpath: list[str] = []
            if old_classpath:
                if isinstance(old_classpath, str):
                    new_classpath.append(old_classpath)
                else:
                    new_classpath.extend(old_classpath)
            query_classpath = new_query_options.pop("classpath")
            if query_classpath:
                if isinstance(query_classpath, str):
                    new_classpath.append(query_classpath)
                else:
                    new_classpath.extend(query_classpath)
            options["classpath"] = new_classpath
        if "jdbc_driver_args" in options:
            jdbc_driver_args = options.pop("jdbc_driver_args")
            if isinstance(jdbc_driver_args, str):
                new_query_options["jdbc_driver_args"] = jdbc_driver_args
            else:
                new_query_options["jdbc_driver_args"] = self.json_serializer(jdbc_driver_args)
        return database_url_new.replace(driver=None, options=new_query_options), options

    async def connect(self, database_url: DatabaseURL, **options: Any) -> None:
        classpath: str | list[str] | None = options.pop("classpath", None)
        if classpath:
            if isinstance(classpath, str):
                classpath = [classpath]
            parent_dir = Path(os.getcwd())
            for clpath in classpath:
                _classpath: str = str(clpath)
                if _classpath not in seen_classpathes:
                    seen_classpathes.add(_classpath)
                    # add original
                    try:
                        addClassPath(parent_dir.joinpath(clpath))
                    except Exception as exc:
                        raise Exception(str(exc)) from None

        if not isJVMStarted():
            startJVM()
        await super().connect(database_url, **options)
