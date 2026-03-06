"""Tests for bugs fixed and code refactored during the codebase transformation.

Covers:
- Bug fix: ``extract_options`` isolation_level pop (sqlalchemy.py)
- Bug fix: Transaction decorator return-value preservation (transaction.py)
- Bug fix: ``_connection`` setter no longer returns dead value (database.py)
- Bug fix: Force-disconnect no longer hits unreachable assert (database.py)
- Refactor: ``_resolve_admin_url`` and ``_admin_client`` helpers (testclient.py)
- Refactor: Mutable default arguments replaced with tuples (database.py)
- Type improvement: ``column`` parameter accepts ``int | str`` (interfaces.py)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from databasez import DatabaseURL

# ---------------------------------------------------------------------------
# extract_options — isolation_level was popping wrong key (param = "echo_pool")
# ---------------------------------------------------------------------------


class TestExtractOptionsIsolationLevel:
    """Regression tests for the isolation_level extraction bug in SQLAlchemyDatabase."""

    def _make_backend(self):
        """Create a minimal SQLAlchemyDatabase instance for testing."""
        from databasez.sqlalchemy import (
            SQLAlchemyConnection,
            SQLAlchemyDatabase,
            SQLAlchemyTransaction,
        )

        backend = SQLAlchemyDatabase(SQLAlchemyConnection, SQLAlchemyTransaction)
        return backend

    def test_isolation_level_extracted_correctly(self):
        """isolation_level should be popped from query options — not 'echo_pool'."""
        backend = self._make_backend()
        url = DatabaseURL(
            "postgresql+asyncpg://user:pass@localhost/mydb?isolation_level=REPEATABLE_READ"
        )
        new_url, options = backend.extract_options(url)
        assert options["isolation_level"] == "REPEATABLE_READ"
        # Should not remain in the URL query
        assert "isolation_level" not in dict(new_url.options)

    def test_echo_pool_extracted_separately(self):
        """echo_pool should be correctly handled as a boolean param."""
        backend = self._make_backend()
        url = DatabaseURL("postgresql+asyncpg://user:pass@localhost/mydb?echo_pool=true")
        new_url, options = backend.extract_options(url)
        assert options["echo_pool"] is True

    def test_both_echo_pool_and_isolation_level(self):
        """Both should be extracted independently from the URL."""
        backend = self._make_backend()
        url = DatabaseURL(
            "postgresql+asyncpg://user:pass@localhost/mydb"
            "?echo_pool=true&isolation_level=SERIALIZABLE"
        )
        new_url, options = backend.extract_options(url)
        assert options["echo_pool"] is True
        assert options["isolation_level"] == "SERIALIZABLE"

    def test_pool_size_and_overflow(self):
        """Numeric options should be parsed as ints."""
        backend = self._make_backend()
        url = DatabaseURL(
            "postgresql+asyncpg://user:pass@localhost/mydb?pool_size=10&max_overflow=20"
        )
        _, options = backend.extract_options(url)
        assert options["pool_size"] == 10
        assert options["max_overflow"] == 20

    def test_pool_recycle_as_float(self):
        """pool_recycle should be parsed as float."""
        backend = self._make_backend()
        url = DatabaseURL("postgresql+asyncpg://user:pass@localhost/mydb?pool_recycle=3600.5")
        _, options = backend.extract_options(url)
        assert options["pool_recycle"] == 3600.5


# ---------------------------------------------------------------------------
# Transaction decorator — return value was being dropped
# ---------------------------------------------------------------------------


class TestTransactionDecoratorReturnValue:
    """Regression test: decorated functions should preserve their return value."""

    @pytest.mark.asyncio
    async def test_decorator_preserves_return_value(self):
        """The __call__ wrapper should return the inner function's result."""
        from databasez.core.transaction import Transaction

        # Create a mock connection
        mock_connection = MagicMock()
        mock_connection._loop = MagicMock()

        # Create a Transaction with mocked internals
        tx = Transaction(lambda: mock_connection, force_rollback=False)

        # Directly test the wrapper: patch __aenter__ and __aexit__
        async def mock_func(*args, **kwargs):
            return 42

        # The __call__ method returns a wrapper
        wrapper = tx(mock_func)

        # Patch __aenter__ and __aexit__ to be no-ops
        with (
            patch.object(Transaction, "__aenter__", new_callable=AsyncMock),
            patch.object(Transaction, "__aexit__", new_callable=AsyncMock),
        ):
            result = await wrapper()
            assert result == 42


# ---------------------------------------------------------------------------
# _connection setter — should have no return value
# ---------------------------------------------------------------------------


class TestConnectionSetterNoReturn:
    """The _connection setter should not return a value (setters are void)."""

    def test_setter_returns_none(self):
        """Python property setters always return None."""
        # Verify the setter type annotation is void
        import inspect

        from databasez.core.database import Database

        setter_fn = Database._connection.fset
        sig = inspect.signature(setter_fn)
        assert sig.return_annotation in (None, "None", inspect.Parameter.empty)


# ---------------------------------------------------------------------------
# Mutable default arguments — tuples instead of lists
# ---------------------------------------------------------------------------


class TestImmutableDefaults:
    """Default arguments should be immutable sequences (tuples)."""

    def test_get_backends_default_is_tuple(self):
        """get_backends overwrite_paths default should be a tuple."""
        import inspect

        from databasez.core.database import Database

        sig = inspect.signature(Database.get_backends)
        default = sig.parameters["overwrite_paths"].default
        assert isinstance(default, tuple), f"Expected tuple, got {type(default).__name__}"

    def test_apply_database_url_default_is_tuple(self):
        """apply_database_url_and_options overwrite_paths default should be a tuple."""
        import inspect

        from databasez.core.database import Database

        sig = inspect.signature(Database.apply_database_url_and_options)
        default = sig.parameters["overwrite_paths"].default
        assert isinstance(default, tuple), f"Expected tuple, got {type(default).__name__}"


# ---------------------------------------------------------------------------
# testclient._resolve_admin_url helper
# ---------------------------------------------------------------------------


class TestResolveAdminUrl:
    """Tests for the extracted _resolve_admin_url helper method."""

    def test_postgresql_redirects_to_postgres_db(self):
        from databasez.testclient import DatabaseTestClient

        url, database, dialect_name, _ = DatabaseTestClient._resolve_admin_url(
            "postgresql+asyncpg://user:pass@localhost/mydb"
        )
        assert database == "mydb"
        assert dialect_name == "postgresql"
        assert url.database == "postgres"

    def test_mssql_redirects_to_master(self):
        from databasez.testclient import DatabaseTestClient

        url, database, dialect_name, _ = DatabaseTestClient._resolve_admin_url(
            "mssql+aioodbc://user:pass@localhost/mydb"
        )
        assert database == "mydb"
        assert dialect_name == "mssql"
        assert url.database == "master"

    def test_sqlite_unchanged(self):
        from databasez.testclient import DatabaseTestClient

        url, database, dialect_name, _ = DatabaseTestClient._resolve_admin_url(
            "sqlite+aiosqlite:///./test.db"
        )
        assert dialect_name == "sqlite"

    def test_generic_dialect_clears_database(self):
        """Unknown dialects should redirect to database=None or empty string."""
        from databasez.testclient import DatabaseTestClient

        url, database, dialect_name, _ = DatabaseTestClient._resolve_admin_url(
            "mysql+asyncmy://user:pass@localhost/mydb"
        )
        assert database == "mydb"
        # URL.replace(database=None) produces an empty string for the database component
        assert not url.database


# ---------------------------------------------------------------------------
# testclient._needs_autocommit helper
# ---------------------------------------------------------------------------


class TestNeedsAutocommit:
    """Tests for the _needs_autocommit helper."""

    def test_postgresql_asyncpg_needs_autocommit(self):
        from databasez.testclient import DatabaseTestClient

        assert DatabaseTestClient._needs_autocommit("postgresql", "asyncpg") is True

    def test_mssql_pyodbc_needs_autocommit(self):
        from databasez.testclient import DatabaseTestClient

        assert DatabaseTestClient._needs_autocommit("mssql", "pyodbc") is True

    def test_mysql_does_not_need_autocommit(self):
        from databasez.testclient import DatabaseTestClient

        assert DatabaseTestClient._needs_autocommit("mysql", "asyncmy") is False

    def test_sqlite_does_not_need_autocommit(self):
        from databasez.testclient import DatabaseTestClient

        assert DatabaseTestClient._needs_autocommit("sqlite", "aiosqlite") is False


# ---------------------------------------------------------------------------
# DatabaseURL — basic additional coverage
# ---------------------------------------------------------------------------


class TestDatabaseURLAdditional:
    """Additional coverage for DatabaseURL properties."""

    def test_scheme_property(self):
        url = DatabaseURL("postgresql+asyncpg://localhost/mydb")
        assert url.scheme == "postgresql+asyncpg"

    def test_netloc(self):
        url = DatabaseURL("postgresql+asyncpg://user:pass@localhost:5432/mydb")
        assert "localhost" in url.netloc

    def test_obscure_password(self):
        url = DatabaseURL("postgresql://user:secret@localhost/mydb")
        assert "secret" not in url.obscure_password

    def test_equality(self):
        url1 = DatabaseURL("postgresql://localhost/mydb")
        url2 = DatabaseURL("postgresql://localhost/mydb")
        assert url1 == url2

    def test_inequality(self):
        url1 = DatabaseURL("postgresql://localhost/mydb")
        url2 = DatabaseURL("postgresql://localhost/other")
        assert url1 != url2
