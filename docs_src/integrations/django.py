from django.core.asgi import get_asgi_application

from databasez import Database

database = Database("sqlite+aiosqlite:///foo.sqlite")

# `handle_lifespan=True` allows integration with frameworks that don't
# expose ASGI lifespan events themselves.
application = database.asgi(get_asgi_application(), handle_lifespan=True)
