"""High-level transaction wrapper for databasez.

Provides :class:`Transaction`, the public API for managing database
transactions.  Transactions can be used as async context managers, as
decorators, or driven manually via :meth:`~Transaction.start`,
:meth:`~Transaction.commit` and :meth:`~Transaction.rollback`.

Nested transactions are supported through savepoints when the underlying
driver supports them.

Example:
    >>> async with database.transaction():
    ...     await database.execute(query)
"""

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
    """Proxy that dispatches transaction methods to a foreign event loop.

    Used internally by :func:`~databasez.utils.multiloop_protector` when a
    transaction method is called from a loop that differs from the one the
    connection was created on.

    Args:
        transaction: The :class:`Transaction` to proxy.
        fn: The bound method to invoke.
        args: Positional arguments for *fn*.
        kwargs: Keyword arguments for *fn*.
        timeout: Optional timeout in seconds.
    """

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
        """Await the proxied call with an optional timeout.

        Returns:
            Any: The result of the proxied method.
        """
        return await _arun_with_timeout(self.fn(), self.timeout)

    async def acall(self) -> Any:
        """Schedule :meth:`call` on the transaction's event loop.

        Returns:
            Any: The result, relayed from the foreign loop.
        """
        return await arun_coroutine_threadsafe(
            self.call(), self.transaction._loop, self.transaction.poll_interval
        )

    def __await__(self) -> Any:
        return self.acall().__await__()


class Transaction:
    """High-level transaction object returned by :meth:`Database.transaction`.

    Supports three usage patterns:

    1. **Async context manager**::

           async with database.transaction():
               ...

    2. **Decorator**::

           @database.transaction()
           async def do_work():
               ...

    3. **Manual control**::

           txn = await database.transaction()
           try:
               ...
               await txn.commit()
           except Exception:
               await txn.rollback()

    Attributes:
        async_helper: The helper class used for cross-loop proxying.
    """

    # async helper
    async_helper: type[AsyncHelperTransaction] = AsyncHelperTransaction

    def __init__(
        self,
        connection_callable: Callable[[], Connection | None],
        force_rollback: bool,
        existing_transaction: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialise a new Transaction.

        Args:
            connection_callable: A callable (typically a :func:`weakref.ref`)
                that returns the owning :class:`Connection`, or ``None`` if
                the connection has been garbage-collected.
            force_rollback: If ``True``, the transaction will always roll
                back on exit (useful for testing).
            existing_transaction: An already-started backend transaction to
                wrap.  ``None`` means a new transaction will be created on
                :meth:`start`.
            **kwargs: Extra options forwarded to the backend's
                :meth:`~TransactionBackend.start`.
        """
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs
        self._existing_transaction = existing_transaction

    @property
    def connection(self) -> Connection:
        """Return the owning :class:`Connection`.

        Returns:
            Connection: The active connection.

        Raises:
            AssertionError: If the connection has been terminated.
        """
        # Returns the same connection if called multiple times.
        conn = self._connection_callable()
        assert conn is not None, "Connection was terminated. No connection was found"
        return conn

    @property
    def _loop(self) -> asyncio.AbstractEventLoop | None:
        """The event loop of the owning connection."""
        return self.connection._loop

    @property
    def poll_interval(self) -> float:
        """The poll interval of the owning connection."""
        return self.connection.poll_interval

    async def __aenter__(self) -> Transaction:
        """Enter the transaction context and start it if necessary.

        When wrapping an *existing_transaction*, the caller is responsible
        for calling :meth:`start` at the appropriate time.

        Returns:
            Transaction: ``self``.
        """
        if self._existing_transaction is None:
            await self.start(cleanup_on_error=False)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exit the transaction context, committing or rolling back.

        Rolls back if an exception occurred or if *force_rollback* is set;
        otherwise commits.
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> Generator[None, None, Transaction]:
        """Allow ``txn = await database.transaction()`` usage.

        Returns:
            Generator: An awaitable that resolves to ``self`` after
                :meth:`start` completes.
        """
        return self.start().__await__()

    def __call__(self, func: _CallableType) -> _CallableType:
        """Use the transaction as a decorator.

        Args:
            func: An async function to wrap.

        Returns:
            _CallableType: A wrapped function that runs inside this
                transaction.
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper  # type: ignore

    # Called directly from connection.
    @multiloop_protector(False)
    async def _start(
        self,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Internal: begin the backend transaction (loop-protected).

        Creates the backend transaction object, optionally wrapping an
        existing driver transaction, and pushes it onto the connection's
        transaction stack.
        """
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

    async def start(
        self,
        timeout: float | None = None,
        cleanup_on_error: bool = True,
    ) -> Transaction:
        """Begin the transaction and return ``self``.

        Acquires the underlying connection (entering the connection context
        if this transaction is not the connection's own
        ``connection_transaction``) and then delegates to :meth:`_start`.

        Args:
            timeout: Optional timeout for the operation.
            cleanup_on_error: If ``True`` (default), exit the connection
                context on failure.

        Returns:
            Transaction: ``self``, ready for use.

        Raises:
            BaseException: Any exception from the backend's ``start``.
        """
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
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Commit the transaction.

        Pops this transaction from the connection's stack and commits the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
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
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Roll back the transaction.

        Pops this transaction from the connection's stack and rolls back the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
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
