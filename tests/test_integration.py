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

from databasez import Database, DatabaseURL
from tests.test_databases import DATABASE_URLS

metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    # Create test databases
    for url in DATABASE_URLS:
        database_url = str(DatabaseURL(url))
        database_url = (
            database_url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )

        engine = sqlalchemy.create_engine(database_url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    for url in DATABASE_URLS:
        database_url = str(DatabaseURL(url))
        database_url = (
            database_url.replace("sqlite+aiosqlite:", "sqlite:")
            .replace("mssql+aioodbc:", "mssql+pyodbc:")
            .replace("postgresql+asyncpg:", "postgresql+psycopg:")
            .replace("mysql+asyncmy:", "mysql+pymysql:")
            .replace("mysql+aiomysql:", "mysql+pymysql:")
        )

        engine = sqlalchemy.create_engine(database_url)
        metadata.drop_all(engine)


def get_app(database_url):
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
def test_integration(database_url):
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


@pytest.mark.parametrize("database_url", DATABASE_URLS)
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
