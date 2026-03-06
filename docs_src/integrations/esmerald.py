from ravyn import Ravyn

from databasez import Database

database = Database("sqlite+aiosqlite:///foo.sqlite")


app = Ravyn(routes=[])


@app.on_event("startup")
async def startup() -> None:
    await database.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await database.disconnect()
