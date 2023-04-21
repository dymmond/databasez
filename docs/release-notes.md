# Release Notes

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
