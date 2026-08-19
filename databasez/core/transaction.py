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
    from databasez import interfaces

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


class BoundTransaction:
    def __init__(
        self,
        connection: Connection,
        transaction_db: interfaces.TransactionBackend,
        transaction: Transaction,
        parent: None | Transaction = None,
        unmanaged: bool = False,
    ):
        """
        Transaction helper around interfaces.TransactionBackend.

        Internal glue component between connections and transactions.
        """
        self.connection = connection
        self.transaction_db = transaction_db
        self.transaction_db.owner = self
        self.transaction = transaction
        self.parent = parent
        # tracks if the bound transaction was finalized
        self.is_finalizing = False
        self.unmanaged = unmanaged

    async def _begin_finalize(self) -> interfaces.TransactionBackend:
        """Prepare this transaction for commit/rollback.

        For nested transactions started in the same task, out-of-order finalization
        remains an error. For sibling transactions from other tasks, wait until this
        transaction reaches the top of the stack.

        Returns:
            TransactionBackend: Backend transaction
        """
        if self.is_finalizing:
            raise RuntimeError("Transaction is already being finalized")
        connection = self.connection
        while True:
            async with connection._transaction_lock:
                _, own, is_top = await self.transaction._get_parent_and_bound(connection)
                if own is None:
                    raise RuntimeError("Transaction is not active") from None
                if is_top:
                    self.is_finalizing = True
                    return self.transaction_db
                await connection._transaction_notifier.wait()

    async def _finish_finalize(self) -> None:
        """Remove this transaction from the stack after backend finalize."""
        connection = self.connection
        transaction = self.transaction
        try:
            async with connection._transaction_lock:
                for index in range(len(connection._transaction_stack) - 1, -1, -1):
                    if connection._transaction_stack[index] is self:
                        connection._transaction_stack.pop(index)
                        break
                connection._current_transaction = self.parent
                connection._transaction_notifier.notify_all()
        finally:
            del self.parent
            del self.connection
            del self.transaction
            del self.transaction_db
            # decrease connection counter, when not the connection_transaction
            if connection.connection_transaction is not transaction:
                await connection.__aexit__()

    async def commit(
        self,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Commit the transaction.

        Pops this transaction from the connection's stack and commits the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
        transaction = await self._begin_finalize()
        try:
            await transaction.commit()
        finally:
            await self._finish_finalize()

    async def rollback(
        self,
    ) -> None:
        """Roll back the transaction.

        Pops this transaction from the connection's stack and rolls back the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
        transaction = await self._begin_finalize()
        try:
            await transaction.rollback()
        finally:
            await self._finish_finalize()


class Transaction:
    """High-level transaction object returned by :meth:`Database.transaction`.

    Supports three usage patterns:

    1. **Async context manager**::

           async with database.transaction():
               ...

    2. **Decorator**::

           @database.transaction()
           async def do_work(): ...

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
        **kwargs: Any,
    ) -> None:
        """Initialise a new Transaction.

        Args:
            connection_callable: A callable (typically a :func:`weakref.ref`)
                that returns the owning :class:`Connection`, or ``None`` if
                the connection has been garbage-collected.
            force_rollback: If ``True``, the transaction will always roll
                back on exit (useful for testing).
            **kwargs: Extra options forwarded to the backend's
                :meth:`~TransactionBackend.start`.
        """
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs

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

    async def _get_parent_and_bound(
        self, connection: Connection
    ) -> tuple[BoundTransaction | None, BoundTransaction | None, bool]:
        """
        Return the assoziated parent BoundTransaction, this BoundTransaction and if the parent was the top.

        WARNING: needs transaction_lock held.
        """
        assert connection._loop is asyncio.get_running_loop()
        assert connection._transaction_lock.locked(), "transaction_lock not held"
        parent: BoundTransaction | None = None
        parent_is_top: bool = False
        if not connection._transaction_stack:
            # top because root in this connection
            return None, None, True
        for num, bound in enumerate(connection._transaction_stack):
            parent_is_top = False
            if bound.transaction is self:
                # check if self is top
                return parent, bound, num == len(connection._transaction_stack) - 1
            # must be second
            parent = bound
            parent_is_top = True
        return parent, None, parent_is_top

    async def get_bound_transaction(self, connection: Connection) -> BoundTransaction | None:
        async with connection._transaction_lock:
            return (await self._get_parent_and_bound(connection))[1]

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
        await self.start(cleanup_on_error=False)
        return self

    # Called directly from connection.
    @multiloop_protector(False)
    async def _aexit(self, rollback: bool) -> None:
        bound = await self.get_bound_transaction(self.connection)
        if bound is None or bound.is_finalizing or bound.unmanaged:
            # not an error
            return
        if rollback:
            await bound.rollback()
        else:
            await bound.commit()

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
        await self._aexit(rollback=exc_type is not None or self._force_rollback)

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
            transaction = type(self)(
                self._connection_callable,
                force_rollback=self._force_rollback,
                **self._extra_options,
            )
            async with transaction:
                return await func(*args, **kwargs)

        return wrapper  # type: ignore

    # Called directly from connection.
    @multiloop_protector(False)
    async def _start(
        self,
        *,
        # required for multiple parallel transactions on the same connection
        parent_transaction: Transaction | None,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> BoundTransaction:
        """Internal: begin the backend transaction (loop-protected).

        Creates the backend transaction object, optionally wrapping an
        existing driver transaction, and pushes it onto the connection's
        transaction stack.
        """
        connection = self.connection
        assert connection._loop is asyncio.get_running_loop()

        async with connection._transaction_lock:
            while True:
                parent, own, is_parent_top = await self._get_parent_and_bound(connection)
                stack_size = len(connection._transaction_stack)
                if own is not None:
                    # reenter self
                    return own
                if is_parent_top and (
                    parent is None
                    or parent_transaction is None
                    or parent.transaction is parent_transaction
                    # this checks if the parent is the connection transaction
                    # Fixes problem: when parent_transaction is None is not correct
                    # FIXME: the former problem shouldn't happen, this clause should be not needed
                    # or parent.transaction is connection.connection_transaction
                ):
                    break
                await connection._transaction_notifier.wait()
            is_root = stack_size == 0 or (
                stack_size == 1 and connection.connection_transaction is not None
            )

            # we retrieve the base connection here, loop protection is required
            _transaction = connection._get_connection_backend().transaction(None)
            await _transaction.start(is_root=is_root, **self._extra_options)
            bound = BoundTransaction(
                connection=connection,
                transaction_db=_transaction,
                transaction=self,
                parent=parent_transaction,
            )
            connection._transaction_stack.append(bound)
            connection._current_transaction = self
            return bound

    async def start(
        self,
        *,
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
        parent_transaction = connection._current_transaction
        # we have a loop now in case of full_isolation
        try:
            await self._start(timeout=timeout, parent_transaction=parent_transaction)
        except BaseException:
            # normal start call
            if (
                cleanup_on_error
                and getattr(connection, "connection_transaction", None) is not self
            ):
                await connection.__aexit__()
            raise
        return self

    @multiloop_protector(False)
    async def commit(
        self,
        *,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Commit the transaction.

        Pops this transaction from the connection's stack and commits the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
        connection = self.connection
        bound = await self.get_bound_transaction(connection)
        if bound is None or bound.is_finalizing:
            raise RuntimeError("Transaction is not active")
        await bound.commit()

    @multiloop_protector(False)
    async def rollback(
        self,
        *,
        timeout: float | None = None,  # stub for multiloop_protector
    ) -> None:
        """Roll back the transaction.

        Pops this transaction from the connection's stack and rolls back the
        backend transaction.  If this is not the connection's own
        ``connection_transaction``, the connection context is also exited.
        """
        connection = self.connection
        bound = await self.get_bound_transaction(connection)
        if bound is None or bound.is_finalizing:
            raise RuntimeError("Transaction is not active")
        await bound.rollback()
