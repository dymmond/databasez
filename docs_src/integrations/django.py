from django.core.asgi import get_asgi_application

from databasez import Database

applications = Database("sqlite:///foo.sqlite").asgi(
    # except you have a lifespan handler in django
    handle_lifespan=True
)(get_asgi_application())
