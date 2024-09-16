from esmerald import Esmerald

from databasez import Database

database = Database("sqlite:///foo.sqlite")


app = Esmerald(routes=[])


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
