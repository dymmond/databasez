# Release Notes


## 0.10.0


### Added

- `full_isolation` parameter. Isolate the force_rollback Connection in a thread.


## 0.9.7

### Added

- It is now possible to use connect(), disconnect() instead of a async contextmanager in multi-loop calls (multithreading).

### Fixed

- Database calls are forwarded to subdatabase when possible. This unbreaks using not the returned database object.
- `force_rollback` works also in multi-loop call (multithreading).

## 0.9.6

### Fixed

- Databasez is now threadsafe (and multiloop safe).


## 0.9.5

### Changed

- Extract more options by default from query options.

### Fixed

- `disconnect_hook` was called too early.
- `connect_hook` was called too late.
- `pos` not passed through in fetch_one.

## 0.9.4

### Changed

- Implement iterate in sqlalchemy base class (before it was a forward to batched_iterate).

### Fixed

- iterate and batched_iterate misconfigure connection.

## 0.9.3

### Added

- Add lazy_setup parameter for testclient.
- `disconnect` and `connect` return if they performed the setup/cleanup.
- Add backward compatible overwrites of defaults to the TestClient.

### Fixed

- Restore eager setup behavior in testclient.
- Fix drop_dabase with postgresql.
- Fix setup/disconnect hangups.
- Fix ancient docs deps.

### Removed

- Dependency on nest_asyncio. It is an archived repo.

## 0.9.2

### Added

- Expose customization hooks for disconnects, connects.

### Fixed

- Testclient has issues with missing permissions.
- Lazy global connection.

## 0.9.1

### Added

- Expose customization hooks for execute result parsing for subclasses of sqlalchemy.

### Fixed

- `execute_many` works now efficient and properly and returns autoincrement column ids.
- `execute` returned not autoincrement id but defaults.

## 0.9.0

### Added

- `force_rollback` is now a descriptor returning an extensive ForceRollback object.
    - Setting True, False, None is now possible for overwriting the value/resetting to the initial value (None).
    - Deleting it resets it to the initial value.
    - Its trueness value evaluates to the current value, context-sensitive.
    - It still can be used as a contextmanager for temporary overwrites.

### Fixed

- Fixed refcount for global connections.

### Changed

- `connect`/`disconnect` calls are now refcounted. Nesting is now supported.
- ACTIVE_TRANSACTIONS dict is not replaced anymore when initialized.


## 0.8.5

### Added

- Allow overwriting `force_rollback` when copying via passing Database as parameter.

### Changed

- The `force_rollback` parameter is now None by default.

### Fixed

- Fix typo in Testclient copying path.

## 0.8.4

### Added

- Provide `__copy__` method for Database, DatabaseBackend.
- Database is now an allowed type for Database (used for copying). Same is true for the DatabaseTestClient subclass.

### Fixed

- Speed of tests.
- DATABASE_CONFIG_URLS had passwords not properly unquoted.

## 0.8.3

### Added

- DatabaseTestClient has now an option for changing the test prefix.

### Fixed

- DatabaseTestClient can now use the improvements in 0.8.0 too.
- DatabaseTestClient is now tested.
- Fix run_sync on Database objects.

## 0.8.2

### Fixed

- Cannot pass parameters to execute directly. Bulk_inserts not possible anymore.

## 0.8.1

### Fixed

- The `dbapi2` dialect used only a single thread for all connections.
- Some options were not translated into query options and vice versa.

## 0.8.0

### Added

- `batched_iterate`.
- `jdbc` dialect to load nearly all jdbc drivers (note: many features won't work).
- `dbapi2` dialect to load nearly all dbapi2 drivers (note: many features won't work).

### Changed

- Use psycopg3 by default. Autoupgrade `postgres://` to `postgres+psycopg://`. Note: this behavior differs from sqlalchemy which still uses psycopg2 by default.
- `fetch_all` uses now iterate as fallback.
- `hatch` is now used for release management, cleaning.
- Use mariadb for tests instead of mysql (resource problems).

### Removed

- `aiopg` support (no update for 2 years and better alternatives available).
- Remove Makefile and other scripts.
- Own `run_sync` implementation. Use asyncio.run instead.

### Fixed

- `docker compose` doesn't uses that much resources anymore.

## 0.7.2

### Fixed

- Regression introduced by asyncio with `DatabaseTestClient`.

## 0.7.1

### Fixed

- Fix regression introduced by SQLAlchemy 2.0.25 with `make_url`.

## 0.7.0

### Fixed

- `urllib.parse.urlsplit` was causing the password or username from being properly parsed and
split with special characters.

## 0.6.0

### Added

- Support for Python 3.12

### Changed

* `pyodbc` version for MSSQL driver.

## 0.5.0

### Fixed

- Patch done in the core of Databases fixing the concurrent usage of connections and transactions.
This patch also affects databases. [#PR 546](https://github.com/encode/databases/pull/546) by [@zevisert](https://github.com/zevisert).
We thank [@zevisert](https://github.com/zevisert) for the fix done in the original project that also affect Databasez.

## 0.4.0

### Changed

- Added extra support for unix sockets for mysql and asyncmy. PR [#13](https://github.com/dymmond/databasez/pull/13) by [tarsil](https://github.com/tarsil).
- Update version of SQLAlchemy.

## 0.3.0

### Changed

- Upgraded SQLAlchemy to version 2.0.12+ where the BaseRow
implementation was redesigned and improved in terms of performance. [#10](https://github.com/dymmond/databasez/pull/10)
- Updated internal Record representation of the returned Row from SQLAlchemy
reflecting the performance improvements.

## 0.2.2

### Fixed

- Bad state when a connection was cancelled.

## 0.2.1

### Fixed

- Error raised when checking the column mapping for empty columns.

## 0.2.0

### Changed

- Updated requirements to the latest of sqlalchemy and added support for `nest_asyncio`.

### Added

- New `run_sync` function for connections allowing every connection to run blocking operations
inside async. For example, a `sqlalchemy inspect`

## 0.1.0

Initial release.

This is the official release of `databasez` where it provides the experience as the one
forked from [Encode](https://github.com/encode/databases) with additional features and improvements.

* SQLAlchemy 2+ integration.
* Additonal support for `mssql`.
* [Connection as dict](./connections-and-transactions.md#connection-options-as-a-dictionary).
* Brings a native test client.
