import typing

from jpype import addClassPath, isJVMStarted, startJVM

from databasez.sqlalchemy import SQLAlchemyDatabase

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


seen_classpathes: typing.Set[str] = set()


class Database(SQLAlchemyDatabase):
    def extract_options(
        self,
        database_url: "DatabaseURL",
        **options: typing.Dict[str, typing.Any],
    ) -> typing.Tuple["DatabaseURL", typing.Dict[str, typing.Any]]:
        database_url_new, options = super().extract_options(database_url, **options)
        new_query_options = dict(database_url.options)
        if database_url_new.driver:
            new_query_options["jdbc_dsn_driver"] = database_url_new.driver
        return database_url_new.replace(driver=None, options=new_query_options), options

    async def connect(self, database_url: "DatabaseURL", **options: typing.Any) -> None:
        if not isJVMStarted():
            startJVM()
        classpath: typing.Optional[str] = options.pop("classpath", None)
        if classpath:
            _classpath: str = str(classpath)
            if _classpath not in seen_classpathes:
                seen_classpathes.add(_classpath)
                # add original
                addClassPath(classpath)

        await super().connect(database_url, **options)
