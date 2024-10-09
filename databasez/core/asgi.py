from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databasez.core.Database import Database

ASGIApp = Callable[
    [
        dict[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
    ],
    Awaitable[None],
]


class MuteInteruptException(BaseException):
    pass


@dataclass
class ASGIHelper:
    app: ASGIApp
    database: Database
    handle_lifespan: bool = False

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
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
