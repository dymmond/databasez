# Integrations

Databasez has several ways to integrate in applications. Mainly recommended
are the async contextmanager one and the asgi based.

## AsyncContextManager

The recommended way of manually using databasez is via the async contextmanager protocol.
This way it is ensured that the database is tore down on errors.

Luckily starlette based apps support the lifespan protocol (startup, teardown of an ASGI server) via async contextmanagers.

```python
{!> ../docs_src/integrations/starlette.py !}
```

Note: This works also in different domains which are not web related.


## ASGI

This is a lifespan protocol interception shim for ASGI lifespan. Instead of using the lifespan parameter of starlette, it is possible
to wrap the ASGI application via the shim. This way databasez intercepts lifespans requests and injects its code.
By default it passes the lifespan request further down, but it has a compatibility option named `handle_lifespan`.
It is required for ASGI apps without lifespan support like django.


```python
{!> ../docs_src/integrations/django.py !}
```

## Manually

Some Server doesn't support the lifespan protocol. E.g. WSGI based servers. Here is an example how to integrate it.
As well as with the AsyncContextManager we are not limitted to web applications.

```python
{!> ../docs_src/integrations/esmerald.py !}
```
