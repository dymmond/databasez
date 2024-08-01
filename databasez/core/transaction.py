from __future__ import annotations

import functools
import typing
import weakref
from contextvars import ContextVar
from types import TracebackType

from databasez import interfaces

if typing.TYPE_CHECKING:
    from .connection import Connection


_CallableType = typing.TypeVar("_CallableType", bound=typing.Callable)


ACTIVE_TRANSACTIONS: ContextVar[
    typing.Optional[weakref.WeakKeyDictionary[Transaction, interfaces.TransactionBackend]]
] = ContextVar("ACTIVE_TRANSACTIONS", default=None)


class Transaction:
    def __init__(
        self,
        connection_callable: typing.Callable[[], typing.Optional[Connection]],
        force_rollback: bool,
        existing_transaction: typing.Optional[typing.Any] = None,
        **kwargs: typing.Any,
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
    def _transaction(self) -> typing.Optional[interfaces.TransactionBackend]:
        transactions = ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            return None

        return transactions.get(self, None)

    @_transaction.setter
    def _transaction(
        self, transaction: typing.Optional[interfaces.TransactionBackend]
    ) -> typing.Optional[interfaces.TransactionBackend]:
        transactions = ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            transactions = weakref.WeakKeyDictionary()
        else:
            transactions = transactions.copy()

        if transaction is None:
            transactions.pop(self, None)
        else:
            transactions[self] = transaction

        ACTIVE_TRANSACTIONS.set(transactions)
        return transactions.get(self, None)

    async def __aenter__(self) -> Transaction:
        """
        Called when entering `async with database.transaction()`
        """
        if self._existing_transaction is None:
            await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> typing.Generator[None, None, Transaction]:
        """
        Called if using the low-level `transaction = await database.transaction()`
        """
        return self.start().__await__()

    def __call__(self, func: _CallableType) -> _CallableType:
        """
        Called if using `@database.transaction()` as a decorator.
        """

        @functools.wraps(func)
        async def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper  # type: ignore

    async def start(self) -> Transaction:
        async with self.connection._transaction_lock:
            is_root = not self.connection._transaction_stack
            _transaction = self.connection._connection.transaction(self._existing_transaction)
            _transaction.owner = self
            await self.connection.__aenter__()
            if self._existing_transaction is None:
                await _transaction.start(is_root=is_root, **self._extra_options)
            self._transaction = _transaction
            self.connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        async with self.connection._transaction_lock:
            _transaction = self._transaction
            if _transaction is not None:
                self._transaction = None
                assert self.connection._transaction_stack[-1] is self
                self.connection._transaction_stack.pop()
                await _transaction.commit()
                await self.connection.__aexit__()

    async def rollback(self) -> None:
        async with self.connection._transaction_lock:
            _transaction = self._transaction
            if _transaction is not None:
                self._transaction = None
                assert self.connection._transaction_stack[-1] is self
                self.connection._transaction_stack.pop()
                await _transaction.rollback()
                await self.connection.__aexit__()
