# Release Notes

## 0.3.0

### Changed

- Upgraded SQLAlchemy to version 2.0.12+ where the BaseRow
implementation was redesigned and improved in terms of performance. [#10](https://github.com/tarsil/databasez/pull/10)
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
