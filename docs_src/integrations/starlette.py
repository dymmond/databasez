import contextlib

from starlette.applications import Starlette

from databasez import Database

database = Database("sqlite+aiosqlite:///foo.sqlite")


@contextlib.asynccontextmanager
async def lifespan(app):  # noqa: ANN001
    async with database:
        yield


application = Starlette(
    lifespan=lifespan,
)
