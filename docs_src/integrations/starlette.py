import contextlib

from starlette.applications import Starlette

from databasez import Database

database = Database("sqlite:///foo.sqlite")


@contextlib.asynccontextmanager
async def lifespan(app):
    async with database:
        yield


application = Starlette(
    lifespan=lifespan,
)
