# Integrations

## Application integration

Databasez has several ways to integrate in applications. Mainly recommended
are the async contextmanager one and the asgi based.

### AsyncContextManager

The recommended way of manually using databasez is via the async contextmanager protocol.
This way it is ensured that the database is torn down on errors.

Luckily starlette based apps support the lifespan protocol (startup, teardown of an ASGI server) via async contextmanagers.

```python
{!> ../docs_src/integrations/starlette.py !}
```

!!! Note
    This works also in different domains which are not web related.


### ASGI

This is a lifespan protocol interception shim for ASGI lifespan. Instead of using the lifespan parameter of starlette, it is possible
to wrap the ASGI application via the shim. This way databasez intercepts lifespans requests and injects its code.
By default it passes the lifespan request further down, but it has a compatibility option named `handle_lifespan`.
It is required for ASGI apps without lifespan support like django.


```python
{!> ../docs_src/integrations/django.py !}
```

### Manually

Some Server doesn't support the lifespan protocol. E.g. WSGI based servers.
As well as with the AsyncContextManager we are not limited to web applications.
For demonstration purposes we use here esmerald with `on_startup` and `on_shutdown` events.

```python
{!> ../docs_src/integrations/esmerald.py !}
```

!!! Note
    Starlette is deprecating the integration via events.
    If you like this way and wants to keep it you may can switch to
    [Esmerald][esmerald], which continues to support the old way of integration or
    to use [Starlette Bridge][starlette-bridge] which enables such behavior and is from
    the same author.
    [Starlette Bridge][starlette-bridge] works for Starlette and
    any Starlette related project, for example, FastAPI.


## ORMs

There are at least two ORMs using databasez:

- [Edgy][edgy] - A re-development of saffier using pydantic. It is very fast and has more features than saffier.
                 It is the recommended ORM.
- [Saffier][saffier] - A stable workhorse but most development will happen now in edgy.

When using edgy, use the helpers of edgy instead of the ones of databasez for integrating in applications.
This way you get all features of edgy's `Registry`.

## Links

[edgy]: https://github.com/dymmond/edgy
[saffier]: https://github.com/tarsil/saffier
[esmerald]: https://github.com/dymmond/esmerald
[starlette-bridge]: https://github.com/tarsil/starlette-bridge
