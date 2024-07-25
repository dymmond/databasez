import typing

import jpype.dbapi2
from jpype import shutdownJVM, startJVM

from databasez.interfaces import DatabaseBackend

if typing.TYPE_CHECKING:
    from databasez.core.databaseurl import DatabaseURL


class Database(DatabaseBackend):
    async def connect(self, database_url: DatabaseURL, **options: typing.Any) -> None:
        startJVM(classpath=self._database_url.options.get("classpath"))

    async def disconnect(self) -> None:
        shutdownJVM()
