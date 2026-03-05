from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databasez.core.database import Database

ASGIApp = Callable[
    [
        dict[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
    ],
    Awaitable[None],
]
"""Standard ASGI application callable signature."""


class MuteInteruptException(BaseException):
    """Sentinel exception used to suppress lifespan failures without propagating."""

    ...


@dataclass
class ASGIHelper:
    """ASGI middleware that manages database connect/disconnect via lifespan.

    Wraps an existing ASGI application and intercepts ``lifespan.startup``
    and ``lifespan.shutdown`` events to call
    :meth:`~databasez.core.database.Database.connect` and
    :meth:`~databasez.core.database.Database.disconnect`.

    When *handle_lifespan* is ``True`` the helper fully manages the lifespan
    protocol loop instead of forwarding events to the inner application.

    Attributes:
        app: The wrapped ASGI application.
        database: The :class:`Database` instance to manage.
        handle_lifespan: When ``True``, handle the entire lifespan
            protocol internally.
    """

    app: ASGIApp
    database: Database
    handle_lifespan: bool = False

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Handle an ASGI event.

        For ``lifespan`` scopes, intercept startup/shutdown to
        connect/disconnect the database.  All other scopes are forwarded
        to the wrapped application.

        Args:
            scope: The ASGI scope dictionary.
            receive: The ASGI receive callable.
            send: The ASGI send callable.
        """
        if scope["type"] == "lifespan":
            original_receive = receive

            async def receive() -> dict[str, Any]:
                message = await original_receive()
                if message["type"] == "lifespan.startup":
                    try:
                        await self.database.connect()
                    except Exception as exc:
                        await send({"type": "lifespan.startup.failed", "msg": str(exc)})
                        raise MuteInteruptException from None
                elif message["type"] == "lifespan.shutdown":
                    try:
                        await self.database.disconnect()
                    except Exception as exc:
                        await send({"type": "lifespan.shutdown.failed", "msg": str(exc)})
                        raise MuteInteruptException from None
                return message

            if self.handle_lifespan:
                with suppress(MuteInteruptException):
                    while True:
                        message = await receive()
                        if message["type"] == "lifespan.startup":
                            await send({"type": "lifespan.startup.complete"})
                        elif message["type"] == "lifespan.shutdown":
                            await send({"type": "lifespan.shutdown.complete"})
                            return
                return

        with suppress(MuteInteruptException):
            await self.app(scope, receive, send)
