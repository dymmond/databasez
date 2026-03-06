# Integrations

## Application integration

Databasez has three common integration styles:

1. async context manager lifecycle
2. ASGI lifespan wrapper (`database.asgi(...)`)
3. manual startup/shutdown hooks

## Async context manager (recommended)

Use an app lifespan context and keep lifecycle explicit:

```python
{!> ../docs_src/integrations/starlette.py !}
```

This style works for web and non-web async applications.

## ASGI wrapper

`database.asgi(app, handle_lifespan=False)` wraps an ASGI app and injects connect/disconnect on lifespan events.

For frameworks without native lifespan support, use `handle_lifespan=True`:

```python
{!> ../docs_src/integrations/django.py !}
```

## Manual startup/shutdown hooks

Some servers or frameworks use event hooks instead of lifespan context managers.

```python
{!> ../docs_src/integrations/esmerald.py !}
```

## ORMs

There are at least two ORMs using databasez:

- [Edgy][edgy] - recommended and actively developed.
- [Saffier][saffier] - stable, but most new development is in Edgy.

When using Edgy, prefer Edgy's own helpers for application integration so you get full registry features.

## Links

[edgy]: https://github.com/dymmond/edgy
[saffier]: https://github.com/tarsil/saffier
