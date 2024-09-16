import asyncio
import contextlib

import pytest
import sqlalchemy
from esmerald import Gateway, Request, route
from esmerald import JSONResponse as EsmeraldJSONResponse
from esmerald.applications import Esmerald
from esmerald.testclient import EsmeraldTestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from databasez import Database
from tests.shared_db import database_client, stop_database_client
from tests.test_databases import DATABASE_URLS

metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)


@pytest.fixture(params=DATABASE_URLS)
def database_url(request):
    # Create test databases
    loop = asyncio.new_event_loop()
    database = loop.run_until_complete(database_client(request.param, metadata))
    yield str(database.url)
    loop.run_until_complete(stop_database_client(database, metadata))


def get_starlette_app(database_url):
    database = Database(database_url, force_rollback=True)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        async with database:
            yield

    async def list_notes(request):
        query = notes.select()
        results = await database.fetch_all(query)
        content = [{"text": result.text, "completed": result.completed} for result in results]
        return JSONResponse(content)

    async def add_note(request):
        data = await request.json()
        query = notes.insert().values(text=data["text"], completed=data["completed"])
        await database.execute(query)
        return JSONResponse({"text": data["text"], "completed": data["completed"]})

    app = Starlette(
        lifespan=lifespan,
        routes=[
            Route("/notes", endpoint=list_notes, methods=["GET"]),
            Route("/notes", endpoint=add_note, methods=["POST"]),
        ],
    )

    return app


def get_asgi_app(database_url):
    database = Database(database_url, force_rollback=True)

    async def list_notes(request):
        query = notes.select()
        results = await database.fetch_all(query)
        content = [{"text": result.text, "completed": result.completed} for result in results]
        return JSONResponse(content)

    async def add_note(request):
        data = await request.json()
        query = notes.insert().values(text=data["text"], completed=data["completed"])
        await database.execute(query)
        return JSONResponse({"text": data["text"], "completed": data["completed"]})

    app = database.asgi(
        Starlette(
            routes=[
                Route("/notes", endpoint=list_notes, methods=["GET"]),
                Route("/notes", endpoint=add_note, methods=["POST"]),
            ],
        )
    )

    return app


def get_asgi_no_lifespan(database_url):
    database = Database(database_url, force_rollback=True)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        raise

    async def list_notes(request):
        query = notes.select()
        results = await database.fetch_all(query)
        content = [{"text": result.text, "completed": result.completed} for result in results]
        return JSONResponse(content)

    async def add_note(request):
        data = await request.json()
        query = notes.insert().values(text=data["text"], completed=data["completed"])
        await database.execute(query)
        return JSONResponse({"text": data["text"], "completed": data["completed"]})

    app = database.asgi(handle_lifespan=True)(
        Starlette(
            lifespan=lifespan,
            routes=[
                Route("/notes", endpoint=list_notes, methods=["GET"]),
                Route("/notes", endpoint=add_note, methods=["POST"]),
            ],
        )
    )

    return app


def get_esmerald_app(database_url):
    database = Database(database_url, force_rollback=True)

    @route("/notes", methods=["GET"])
    async def list_notes(request: Request) -> EsmeraldJSONResponse:
        query = notes.select()
        results = await database.fetch_all(query)
        content = [{"text": result.text, "completed": result.completed} for result in results]
        return EsmeraldJSONResponse(content)

    @route("/notes", methods=["POST"])
    async def add_notes(request: Request) -> EsmeraldJSONResponse:
        data = await request.json()
        query = notes.insert().values(text=data["text"], completed=data["completed"])
        await database.execute(query)
        return EsmeraldJSONResponse({"text": data["text"], "completed": data["completed"]})

    app = Esmerald(routes=[Gateway(handler=list_notes), Gateway(handler=add_notes)])

    @app.on_event("startup")
    async def startup():
        await database.connect()

    @app.on_event("shutdown")
    async def shutdown():
        await database.disconnect()

    return app


@pytest.mark.parametrize("get_app", [get_starlette_app, get_asgi_app, get_asgi_no_lifespan])
def test_integration(database_url, get_app):
    app = get_app(database_url)

    with TestClient(app) as client:
        response = client.post("/notes", json={"text": "example", "completed": True})
        assert response.status_code == 200
        assert response.json() == {"text": "example", "completed": True}

        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == [{"text": "example", "completed": True}]

    with TestClient(app) as client:
        # Ensure sessions are isolated
        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == []


def test_integration_esmerald(database_url):
    app = get_esmerald_app(database_url)

    with EsmeraldTestClient(app) as client:
        response = client.post("/notes", json={"text": "example", "completed": True})
        assert response.status_code == 200
        assert response.json() == {"text": "example", "completed": True}

        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == [{"text": "example", "completed": True}]

    with EsmeraldTestClient(app) as client:
        # Ensure sessions are isolated
        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == []
