from __future__ import annotations

import asyncio
from collections.abc import Callable, Generator
from functools import partial, wraps
from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeVar

from databasez.utils import _arun_with_timeout, arun_coroutine_threadsafe, multiloop_protector

if TYPE_CHECKING:
    from .connection import Connection


_CallableType = TypeVar("_CallableType", bound=Callable)


class AsyncHelperTransaction:
    def __init__(
        self,
        transaction: Any,
        fn: Callable,
        args: Any,
        kwargs: Any,
        timeout: float | None,
    ) -> None:
        self.transaction = transaction
        self.fn = partial(fn, self.transaction, *args, **kwargs)
        self.timeout = timeout
        self.ctm = None

    async def call(self) -> Any:
        # is automatically awaited
        return await _arun_with_timeout(self.fn(), self.timeout)

    async def acall(self) -> Any:
        return await arun_coroutine_threadsafe(
            self.call(), self.transaction._loop, self.transaction.poll_interval
        )

    def __await__(self) -> Any:
        return self.acall().__await__()


class Transaction:
    # async helper
    async_helper: type[AsyncHelperTransaction] = AsyncHelperTransaction

    def __init__(
        self,
        connection_callable: Callable[[], Connection | None],
        force_rollback: bool,
        existing_transaction: Any | None = None,
        **kwargs: Any,
    ) -> None:
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs
        self._existing_transaction = existing_transaction

    @property
    def connection(self) -> Connection:
        # Returns the same connection if called multiple times
        conn = self._connection_callable()
        assert conn is not None, "Connection was terminated. No connection was found"
        return conn

    @property
    def _loop(self) -> asyncio.AbstractEventLoop | None:
        return self.connection._loop

    @property
    def poll_interval(self) -> float:
        return self.connection.poll_interval

    async def __aenter__(self) -> Transaction:
        """
        Called when entering `async with database.transaction()`
        """
        # when used with existing transaction, please call start if/when required
        if self._existing_transaction is None:
            await self.start(cleanup_on_error=False)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> Generator[None, None, Transaction]:
        """
        Called if using the low-level `transaction = await database.transaction()`
        """
        return self.start().__await__()

    def __call__(self, func: _CallableType) -> _CallableType:
        """
        Called if using `@database.transaction()` as a decorator.
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self:
                await func(*args, **kwargs)

        return wrapper  # type: ignore

    # called directly from connection
    @multiloop_protector(False)
    async def _start(
        self,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> None:
        connection = self.connection
        assert connection._loop

        async with connection._transaction_lock:
            is_root = not connection._transaction_stack
            # we retrieve the base connection here, loop protection is required
            _transaction = connection._get_connection_backend().transaction(
                self._existing_transaction
            )
            _transaction.owner = self
            if self._existing_transaction is None:
                await _transaction.start(is_root=is_root, **self._extra_options)
            # because we have an await before, we need the _transaction_lock
            self._transaction = _transaction
            connection._transaction_stack.append((self, _transaction))
            _transaction = self._transaction

    # called directly from connection
    async def start(
        self,
        timeout: float | None = None,
        cleanup_on_error: bool = True,
    ) -> Transaction:
        connection = self.connection
        # WARNING: we are maybe in the wrong context and get an AsyncDatabaseHelper, so
        # - don't pass down the connection
        # - assume this is not a connection_transaction
        # count up connection and init multithreading-safe the isolation thread
        # benefit 2: setup works with transaction_lock
        if getattr(connection, "connection_transaction", None) is not self:
            await connection.__aenter__()
        # we have a loop now in case of full_isolation
        try:
            await self._start(timeout=timeout)
        except BaseException as exc:
            # normal start call
            if (
                cleanup_on_error
                and getattr(connection, "connection_transaction", None) is not self
            ):
                await connection.__aexit__()
            raise exc
        return self

    @multiloop_protector(False)
    async def commit(
        self,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> None:
        connection = self.connection
        async with connection._transaction_lock:
            # some transactions are tied to connections and are not on the transaction stack
            if connection._transaction_stack and connection._transaction_stack[-1][0] is self:
                _, _transaction = connection._transaction_stack.pop()
                await _transaction.commit()
        # if a connection_transaction, the connection cleans it up in __aexit__
        # prevent loop
        if connection.connection_transaction is not self:
            await connection.__aexit__()

    @multiloop_protector(False)
    async def rollback(
        self,
        timeout: float | None = None,  # stub for type checker, multiloop_protector handles timeout
    ) -> None:
        connection = self.connection
        async with connection._transaction_lock:
            # some transactions are tied to connections and are not on the transaction stack
            if connection._transaction_stack and connection._transaction_stack[-1][0] is self:
                _, _transaction = connection._transaction_stack.pop()
                await _transaction.rollback()
        # if a connection_transaction, the connection cleans it up in __aexit__
        # prevent loop
        if connection.connection_transaction is not self:
            await connection.__aexit__()
