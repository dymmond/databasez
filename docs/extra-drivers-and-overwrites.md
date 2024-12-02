# Extra drivers and overwrites

## Extra drivers

### JDBC

Databasez injects a jdbc driver. You can use it as simple as:

`jdbc+jdbc-dsn-driver-name://dsn?jdbc_driver=?`

or for modern jdbc drivers

`jdbc+jdbc-dsn-driver-name://dsn`

Despite the jdbc-dsn-driver-name is not known by sqlalchemy this works. The overwrites rewrite the URL for sqlalchemy.


!!! Warning
    It seems like injecting classpathes in a running JVM doesn't work properly. If you have more then one jdbc driver,
    make sure all classpathes are specified.


!!! Warning
    The jdbc driver doesn't support setting the isolation_level yet (this is highly db vendor specific).

### Parameters

The jdbc driver supports some extra parameters which will be removed from the query (note: most of them can be also passed via keywords)

* **jdbc_driver** - import path of the jdbc driver (java format). Required for old jdbc drivers.
* **jdbc_driver_args** - additional keyword arguments for the driver. Note: they must be passed in json format. Query only parameter.
* **jdbc_dsn_driver** - Not really required because of the rewrite but if the url only specifies jdbc:// withouth the dsn driver you can set it here manually.

### Direct-only parameters

These parameters are directly set in database as keywords and have no DSN conversion.

* **transform_reflected_names** - Because JDBC drivers  (especially old) do strange name mangling we may need this parameter.
  It has 3 possible values: "upper", "lower", "none" (default). When upper or lower is used, all names are converted in this case.
  This is required for ancient sqlite jdbc drivers.
* **use_code_datatype** - When True the java code type is evaluated instead of parsing the SQL string datatype. By default False.

### dbapi2


Databasez injects a dbapi2 driver. You can use it as simple as:

`dbapi2+foo://dsn`

or simply

`dbapi2://dsn`

!!! Warning
    The dbapi2 driver doesn't support setting the isolation_level yet (this is highly db vendor specific).

### Parameters

The dbapi2 driver supports some extra parameters which will be removed from the query (note: most of them can be also passed via keywords)

* **dbapi_driver_args** - additional keyword arguments for the driver. Note: they must be passed in json format. Query only parameter.
* **dbapi_dsn_driver** - If required it is possible to set the dsn driver here. Normally it should work without. You can use the same trick like in jdbc to provide a dsn driver.
* **dbapi_pool** - thread/process. Default: thread. How the dbapi2. is isolated. Either via ProcessPool or ThreadPool (with one worker).
* **dbapi_force_async_wrapper** - bool/None. Default: None. Figure out if the async_wrapper is required. By setting a bool the wrapper can be enforced or removed.

The dbapi2 has some extra options which cannot be passed via the url, some of them are required:

* **dbapi_path** - Import path of the dbapi2 module. Required

## Overwrites

Overwrites can improve existing dialects or coexist with extra drivers to add some tricks.
For example dialects having a json_serializer/json_deserializer parameter, have it set to orjson for more performance.

Overwrites can consist of three components, which must be classes of the name in a module:

- Database
- Connection
- Transaction

All of them are independent by default and must inherite from:

- DatabaseBackend
- ConnectionBackend
- TransactionBackend

For simplification there are full functional implementations in `databasez.sqlalchemy`.
It is recommended to inherit from there and just adjust the behavior.

You can have a look in `databasez/overwrites` for examples.
