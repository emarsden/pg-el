# Changelog


## [0.62] - 2025-12-26

- New function `pg-table-acl` which returns the access control list for a specified table.

- Type aliases are recognized as type names in `pg-exec-prepared`, in additional to the type names
  present in the `pg_type` system table. This means that you can use `bigint` as an alternative to
  `int8`, `real` as an alternative to `float4`, `bit varying` as an alternative to `bit`, for
  example. The aliases are also the canonical type names as shown by function `pg_typeof`.

- Rename functions `pg-read-attributes`, `pg-read-tuple`, `pg-read-char`, `pg-unread-char`,
  `pg-read-net-int`, `pg-read-int`, `pg-read-chars`, `pg-read-string`, `pg-send-char`,
  `pg-send-string`, `pg-send-octets`, `pg-send-uint`, `pg-send-net-uint` to use the naming
  convention for internal functions (`pg--` prefix).

- Microsoft Windows: add additional sleep using `sleep-for` when waiting for network data. The
  existing calls to `accept-process-output` with a timeout are insufficient on this platform when
  reading large resultsets. Further testing is needed to determine whether this is also necessary on
  other non-Linux platforms like MS-DOS and Darwin.

- Improve parsing of arrays that contain NULL elements: they will correctly be parsed as the
  `pg-null-marker` for arrays of bits, arrays of booleans, arrays of strings.


## [0.61] - 2025-11-22

- Add support for providing a password for authentication as a function, rather than as a string.
  This allows for integration with the auth-source functionality in Emacs, and helps to reduce the
  length of time where passwords remain present in RAM. Patch from @Kaylebor.

- New error class `pg-invalid-sql-statement-name` which is signalled when an invalid name is given
  to a prepared query.

- New error class `pg-invalid-cursor-name` which is signalled when an invalid name is used for a
  cursor.

- Integer arrays and floating point arrays containing NULL values will now be parsed correctly.


## [0.60] - 2025-09-21

- Add support for version 3.2 of the wire protocol, introduced in PostgreSQL v18. The only change
  with respect to the previously supported version 3.0 is the length of the key used to authenticate
  requests to cancel an ongoing query. As for libpq, we default to using version 3.0, because
  several PostgreSQL variants do not support version 3.2 and have not yet implemented the protocol
  version downgrade functionality that is designed into the protocol.

  Version 3.2 of the protocol can be selected by passing `(3 . 2)` as the value for the
  `:protocol-version` argument to `pg-connect-plist` and `pg-connect/direct-tls`, or by using
  a `protocol_version` URL parameter to `pg-connect/uri`.

- New function to establish PostgreSQL connections `pg-connect-plist`. This function is similar to
  `pg-connect`, but takes keyword arguments instead of optional arguments. Function `pg-connect` is
  deprecated. Similarly, the new macro `with-pg-connection-plist` should be used instead of
  `with-pg-connection` in new code.

- Recently introduced function `pg-connect/direct-tls` has been deprecated; use the `:direct-tls`
  option to `pg-connect-plist` instead.

- Add detection code and workarounds for the PostgreSQL variants OpenGauss (by Huawei) and pgsqlite.

- Add parsing support for arrays of time- and date-related objects.

- The input and output buffers used for communication with PostgreSQL are now trimmed when they
  become too large, with only the most recent data retained. The number of octets to retain for each
  buffer can be customized using the variable `pg-connection-buffer-octets`.


## [0.59] - 2025-08-31

- Add detection code and workarounds for the Yellowbrick PostgreSQL variant.

- Add support for parsing an `options` parameter in a connection string or connection URI, or for
  parsing the contents of the `PGOPTIONS` environment variable (as per the [libpq
  behaviour](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)). This
  is an alternative to using elisp code on `pg-new-connection-hook`.

- Fixes for serializing `bpchar` values when they are used by PostgreSQL to represent `CHAR(N)`
  fields (bug reported by @Tekki).

- Change the mechanism used to send messages to PostgreSQL: instead of sending data incrementally we
  accumulate data in a per-connection output buffer and send it in a chunk when `pg-flush` is
  called. This should reduce the number of small fragmented network packets exchanged with the
  PostgreSQL backend, and should improve performance.


## [0.58] - 2025-08-13

- Add serialization support for the `numeric` data type, for arguments to prepared statements.

- Add serialization support for homogeneous arrays when using the extended query protocol. Types such as
  `text[]` (known as `_text` in the `pg_type` system table), `int4[]`, `float4[]`, `float8[]` can now
  be passed as function arguments.

- Fix parsing of the `bpchar` type, which PostgreSQL uses on the wire to represent `CHARACTER(N)`
  objects. These objects would previously be truncated to their first character, but are now parsed
  as a full length string if the length is greater than 1 character, and still as an Emacs Lisp
  character when the length is equal to 1. The returned string includes trailing padding spaces if
  the value inserted is less long than the field width. Bug reported by @Tekki.

- Add serialization support for the `_varchar` data type.

- Add parsing and serialization support for the `_uuid` data type.

- Add workarounds and detection code for the CedarDB PostgreSQL variant.

- New error classes `pg-duplicate-table` and `pg-duplicate-column`, subclasses of
  `pg-programming-error`.

- New error class `pg-sequence-limit-exceeded`, a subclass of `pg-data-error`.


## [0.57] - 2025-07-30

- Fix the large object functionality to work with version 3 of the frontend-backend wire protocol.
  The large object functions use the 64-bit versions of the backend functions (`lo_lseek64`,
  `lo_truncate64` etc.) to support interactions with large objects up to 4TB in size.


## [0.56] - 2025-07-20

- Allow for two successive messages of type `ErrorMessage` (which is unusual, but used by the OctoDB
  variant).

- Detect and implement workarounds for the semi-compatible PostgreSQL variants Vertica, PolarDB,
  and ArcadeDB, and the graph processing extension AgensGraph.


## [0.55] - 2025-06-29

- New error class `pg-duplicate-prepared-statement`.

- New function `pg-connect/direct-tls` to establish a direct TLS connection to PostgreSQL, rather
  than the STARTTLS-like method used in the standard wire protocol. This connection method is
  implemented from PostgreSQL v18 (currently in beta). It requires ALPN support in Emacs, which is
  not yet committed.

- The `pg-sync` function tries a little harder to resynchronize the data stream with the backend, by
  reading and discarding additional message types that cannot lead to data loss.

- Recognize the hosted PostgreSQL provider thenile.dev as variant `thenile` and implement some
  workarounds for its limitations.


## [0.54] - 2025-05-03

- Handle `ParameterStatus` and `NotificationResponse` messages in `pg-fetch`.

- New function `pg-set-client-encoding` to set the client-side encoding for data sent to the
  backend. Calling this function sends an SQL request to the backend telling it of the new client
  encoding, and sets the per-connection client encoding (accessible via `pgcon-client-encoding`). It
  also checks that the requested client encoding is one supported by PostgreSQL. Note that most of
  the PostgreSQL variants only support UTF8 as a client-encoding.

- Implement workaround for `pg-column-default` for Google Spanner and QuestDB.

- Fix for parsing empty arrays.

- New subclass of `pg-error` `pg-transaction-missing` triggered by an attempt to rollback with no
  transaction in progress.

- Add preliminary support for the ReadySet PostgreSQL proxy as a PostgreSQL variant.

- Add preliminary support for the YottaDB Octo database as a PostgreSQL variant.


## [0.53] - 2025-04-19

- In `pg-sync`, try to read the `ReadyForQuery` message sent by the backend.

- Add test code for the PgDog and PgCat sharding connection poolers.

- Implement workarounds for the RisingWave variant in `pg-table-comment` and `pg-column-comment` and
  their companion setf functions.

- Populate our oid<->typname mappings with predefined data for variants that lack a properly populated
  `pg_type` table (this is the case for GreptimeDB, which contains invalud information such as `UInt8`
  <-> 7). Although strictly speaking there is no guarantee that this internal information will
  remain unchanged in future PostgreSQL releases, it is unlikely to change.


## [0.52] - 2025-04-06

- In `pg-fetch-prepared`, close the portal after fetching the tuple data.

- Provide a basic stub implementation for `pg-table-owner` for CrateDB.

- Add code to detect the Greenplum PostgreSQL variant.


## [0.51] - 2025-03-29

- In `pg-connect/uri`, call `url-unhex-string` on user/password only if non-nil. This restores the
  ability to fall back to `PGUSER` and `PGPASSWORD` environment variables. Patch from @akurth.

- Fix bug in `pg-table-comment` function and in the associated setf function.

- Provide an empty implementation of `pg-column-default` and `pg-table-comment` for the YDB variant.

- New error types `pg-invalid-catalog` name and `pg-timeout`.


## [0.50] - 2025-03-22

- Implement new function `pg-column-comment` with a defsetf.

- Improve `cl-print-object` for a connection object when the pid and database slots are unbound.

- Further workarounds in `pg-table-comment` for QuestDB and Spanner variants.

- Add workarounds in `pg-column-comment` for CrateDB and QuestDB.

- Add workaround in `pg-function-p` for QuestDB.

- Add a custom SQL query for `pg-column-autogenerated-p` to handle limitations in the CrateDB
  variant.

- Add workaround for variant YDB in `pg-tables`.


## [0.49] - 2025-03-08

- Implement hex-decoding for the username and password in `pg-connect/uri`.

- New error classes `pg-character-not-in-repertoire` and `pg-plpgsl-error`.

- New error subclasses of `pg-error`: `pg-database-error`, `pg-operational-error`,
 `pg-programming-error`, `pg-data-error`, `pg-integrity-error`. These are superclasses of some of
 the leaf error subclasses: for example the errors related to integrity violations
 `pg-restrict-violation`, `pg-not-null-violation` and `pg-foreign-key-violation` are all subclasses
 of `pg-integrity-error`.

- Filter out system-internal tables in the list returned by `pg-tables` for the Clickhouse variant.

- New function `pg-current-schema` which returns the value of `current_schema()` (or equivalent on
  PostgreSQL variants that do not implement that function).

- Implement custom logic for `pg-table-comment` for the semi-compatible PostgreSQL variant
  CockroachDB.

- Provide parsing and serialization support for the types defined by the vchord_bm25 extension,
  which implements the BM25 ranking algorithm that is useful for information retrieval applications.
  See file `pg-bm25.el`.


## [0.48] - 2025-02-22

- The error hierarchy has been enriched with many subclasses of `pg-error`, distinguishing between
  an SQL syntax error, a division by zero, a numerical overflow, and so on. See the pg-el manual for
  details.

- Added logic to recognize the PostgreSQL variant Materialize.

- Error reporting: if the constraint name field is present, it is saved in the pgerror struct and
  reported to the user.


## [0.47] - 2025-01-25

- New variable `pg-new-connection-hook` contains a list of functions to be run when a new PostgreSQL
  connection is established. Each function will be called with the new connection as the single
  argument. The default value of this variable includes the function `pg-detect-server-variant`,
  which attempts to determine the type of semi-compatible PostgreSQL variant that we are connected
  to.

- New generic function `pg-do-variant-specific-setup` that allows you to specify setup operations to
  run for a particular semi-compatible PostgreSQL variant.

- Added logic to recognize the PostgreSQL variant AlloyDB Omni.


## [0.46] - 2025-01-12

- Fixes to the handling of timezones when parsing and serializing `time` and `timetz` data. Timezone
  information was previously lost during parsing and serialization. Patch from @akurth.

- Further workarounds in `pg-table-column` and `pg-column-default` to tolerate deficiencies in
  the PostgreSQL compatibility of CrateDB and CockroachDB.

- Add a workaround in `pg-schemas` for RisingWave database.


## [0.45] - 2024-12-22

- When supported by Emacs, enable `TCP_NODELAY` on the network connections to PostgreSQL to disable
  Nagle's algorithm (network segments are sent as soon as possible, even when they contain little
  data). This is done by libpq and results in a 12x (!) speedup on some benchmarks. This is a new
  feature available in (currently unreleased) Emacs 31 (bug#74793).

- New function `pg-schemas` which returns the list of the schemas in a PostgreSQL database. Schemas
  are a form of namespace, which can contain elements such as tables, sequences, indexes and views.
  The default schema name is `public`.

- Add support for detecting the RisingWave database, which is compatible with the PostgreSQL wire
  protocol, with certain limitations. For this database, `pgcon-server-variant` returns the symbol
  `risingwave`.

- When parsing timestamp and time data, preserve the fractional part of a second (patch from @akurth).


## [0.44] - 2024-12-04

- Detect the PostgreSQL variant TimescaleDB. Implement a specific SQL query for `pg-tables` for this
  variant to avoid returning TimescaleDB-internal tables alongside user tables.

- Detect the PostgreSQL variant OrioleDB (really an extension that provides an additional storage
  mechanism).

- Implement a specific SQL query for `pg-tables` for the CrateDB variant, to avoid returning system
  tables alongside user tables.

- The serialization function for floating point values accepts non-float numeric values.

- When parsing a PostgreSQL connection URI or connection string, additional environment variables
  `PGHOST`, `PGHOSTADDR`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` and `PGSSLMODE` are used as
  default values when a value has not been explicitly set in the string.


## [0.43] - 2024-10-15

- Fix serialization for `UUID` types in prepared statements.

- The serialization of key/value pairs in an `HSTORE` object now respects the client encoding, so
  will work correctly with non-ASCII characters.

- Improved error reporting in the pg-geometry library (signalling a subclass of pg-error instead of
  triggering an assertion).

- Additional checks on connection startup to identify the PostgreSQL variant IvorySQL (currently a
  very compatible variant with additional Oracle compatibility functions).

- Fix bug in startup sequence exposed by very short usernames (reported by Ákos Kiss aka `ak`).


## [0.42] - 2024-09-21

- Fix serialization and deserialization for `CHARACTER` and `BPCHAR` types for non-ASCII values.
  PostgreSQL stores these as a single octet, an integer < 256. Characters that are below this limit
  but not in the ASCII range (such as many accented characters if your Emacs uses a Latin-1 charset)
  need to be encoded and decoded.

- Add support for parameters `connect_timeout` and (nonstandard extension) `read_timeout` when
  parsing PostgreSQL connection URIs and connection strings.

- Add functionality to detect the “flavour” variant of (potentially semi-compatible) PostgreSQL
  database that we are connected to. The variant is accessible via `pgcon-server-variant` on a
  connection object. Detected variants are 'cratedb, 'xata, 'cockroachdb, 'yugabyte, 'questdb,
  'greptimedb, 'immudb, 'ydb. This functionality is used to work around certain bugs or incomplete
  emulation of PostgreSQL system tables by some of these semi-compatible database implementations.


## [0.41] - 2024-08-31

- User errors in serialization functions (arguments supplied to `pg-exec-prepared` whose type does
  not correspond to the SQL-defined type) now signal an error of type `pg-type-error`, which is a
  subclass of `pg-user-error`, instead of triggering an assertion failure. This means that they can
  be handled using `condition-case`.

- Delete all uses of variable `pg--MAX_MESSAGE_LEN`, because PostgreSQL no longer has such low
  limits on message sizes (the only limit being the 4-octet size fields in many message types).

- Support for TLS authentication using client certificates (see the documentation for function
  `pg-connect`). The `test-certificates` Makefile target in the `test` directory illustrates the
  creation of a working certificate authority and signed client certificates; depending on the value
  of connection parameter `clientcert`, PostgreSQL is careful to check that the `CN` field of the
  client certificate corresponds to the PostgreSQL user you are connecting as.


## [0.40] - 2024-08-22

- Serialization and deserialization support for [JSONPATH
  expressions](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH).
  These expressions are represented in Emacs Lisp as strings.

- Serialization functions now take a second argument `ENCODING` specifying the client-encoding in
  use, in the same way as for deserialization functions.

- The mappings `pg--parser-by-oid`, `pg--oid-by-typname` and `pg--type-name-by-oid` have been moved
  into the pgcon object, rather than being local variables. This makes it possible to connect from
  the same Emacs instance to PostgreSQL-compatible databases that have different OID values for
  builtin or user-defined types.

- `pgcon` objects are now defined using `defclass` from EIEIO, instead of using `cl-defstruct`. This
  makes it possible to customize the way they are printed, making use in an interactive REPL more
  pleasant.


## [0.39] - 2024-07-29

- New function `pg-fetch-prepared` to bind arguments to a prepared statement and fetch results.
  Preparing a statement once then reusing it multiple times with different argument values allows
  you to avoid the overhead of sending and parsing the SQL query and calculating the query plan.
  This is a simple convenience wrapper around functions `pg-bind`, `pg-describe-portal` and
  `pg-fetch`.

- New function `pg-ensure-prepared-statement` that either returns the previously prepared statement
  saved in the prepared statement cache of our PostgreSQL connection, or prepares the statement and
  saves it in the cache. This simplifies the use of cached prepared statements.

- New function `pg-column-autogenerated-p`, which returns non-nil for columns for which you can
  insert a row without specifying a value for the column. That includes columns:
    - with a specified `DEFAULT` (including `SERIAL` columns)
    - specified as `BIGINT GENERATED ALWAYS AS IDENTITY`
    - specified as `GENERATED ALWAYS AS expr STORED` (calculated from other columns)

- Fix serialization for the PostgreSQL `BPCHAR` type.

- Move to GPL v3 or later licence (from GPL-2.0-or-later).


## [0.38] - 2024-07-21

- The network connection timeout functionality is disabled on Microsoft Windows platforms, where it
  does not function correctly. This is implemented by setting the variable `pg-connect-timeout`
  to 0. This setting can also be used to disable the connection timeout on other platforms.

- Fix the deserialization of `TIMESTAMP WITH TIMEZONE` and `TIMESTAMP WITHOUT TIMEZONE` values when
  the timezone is not explicitly specified.

- Preliminary serialization and deserialization support for the types used by the PostGIS extension
  (types `GEOMETRY`, `GEOGRAPHY`, `SPHEROID`, `BOX2D`, `BOX3D`). Some of these types are
  deserialized from the native hex encoding of EWKB returned by PostGIS to text format using the
  `geosop` commandline utility, if the variable `pg-gis-use-geosop` is non-nil (which is the
  default).


## [0.37] - 2024-07-08

- Fix serialization of JSONB parameters in prepared statements.

- Preliminary serialization and deserialization support for the PostgreSQL `POINT`, `LINE`, `LSEG`,
  `BOX`, `PATH` and `POLYGON` geometric types.

- Parsing support for the PostgreSQL `timetz` type (simply parsed as text).

- New function `pg-serialize` converts an Emacs Lisp object to its serialized text format, playing
  the inverse role to `pg-parse`.

- Source code split into multiple source files to improve maintainability.


## [0.36] - 2024-06-23

- New utility function `pg-function-p` which checks whether a function with a specified name is
  defined in PostgreSQL.

- Better support for the dynamic creation of PostgreSQL types. The cache used to map between
  PostgreSQL type names and the corresponding OIDs is now only used for builtin PostgreSQL types,
  which should not change over time. Values of all other PostgreSQL types, in particular new types
  created using `CREATE TYPE`, will be sent over the wire using the pseudo-OID of 0, telling
  PostgreSQL to look up the OID on the backend. This avoids the possibility of invalid cached OID
  values caused by type creation or destruction on other connections to PostgreSQL.

- Parse the PostgreSQL UUID type as text.

- Parse the PostgreSQL XML type as text.

- Fix deserialization for the PostgreSQL BPCHAR type.

- Fix serialization and deserialization for the PostgreSQL VECTOR type used by the `pgvector`
  extension. Vector embeddings can be floating point numbers, not just integers.

- Implement some workarounds better to support CrateDB.


## [0.35] - 2024-06-08

- New variable `pg-read-timeout` allows you to specify a timeout expressed in seconds when reading
  data from PostgreSQL over the network. A complement to the variable `pg-connect-timeout`.

- Accept a `server_version` backend parameter of the form "17beta1", in addition to the standard
  format of the form "16.3". Tests pass with PostgreSQL 17 beta1.


## [0.34] - 2024-05-20

- Add deserialization support for the `tsvector` type, used by the PostgreSQL full text search
  functionality. They will now be parsed into a list of `pg-ts` structures.


## [0.33] - 2024-05-14

- Add serialization support for the PostgreSQL `date` type.

- Add serialization support for the `vector` type used by the pgvector extension.


## [0.32] - 2024-04-14

- Integer datatypes are now parsed with `cl-parse-integer` instead of `string-to-number` to provide
  error detection.

- Add serialization support for the PostgreSQL `timestamp`, `timestamptz` and `datetime` types.

- New feature: logging of SQL queries sent to PostgreSQL on a per-connection basis. Call function
  `pg-enable-query-log` on a connection object, and SQL sent to the backend by `pg-exec` and
  `pg-exec-prepared` will be logged to a buffer named ` *PostgreSQL query log*` (made unique if
  multiple pg-el connections have been made). The name of the buffer is given by accessor function
  `pgcon-query-log` on the connection object.

- New variable `pg-connect-timeout` to set a timeout (in seconds) for attempts to connect to
  PostgreSQL over the network (does not apply to Unix socket connections).


## [0.31] - 2024-03-28

- Add serialization support for the `hstore` datatype from Emacs Lisp hashtables.

- Add support for schema-qualified names for tables and roles. These are names of the form
  `public.tablename` or `username.tablename` (see the PostgreSQL documentation for `CREATE SCHEMA`).
  “Ordinary” table names in the `public` schema can be specified as a simple string. Table names
  with a different schema are represented by `pg-qualified-name` objects (these are cl-defstruct
  objects). Functions that take a table name as an argument (such as `pg-columns` accept either a
  normal string or a `pg-qualified-name` object. Functions that return table names, in particular
  `pg-tables`, will return strings for tables in the normal `public` schema, and `pg-qualified-name`
  objects otherwise.

- Fix bug in the parsing of `pgcon-server-version-major`.


## [0.30] - 2024-03-11

- Add for receiving data in Emacs using the COPY protocol, as a complement to the existing
  functionality which uses the COPY protocol to send data from Emacs to PostgreSQL. This allows you
  to dump a PostgreSQL table or query result in TSV or CSV format into an Emacs buffer. See function
  `pg-copy-to-buffer`.

- Preliminary implementation of connection to PostgreSQL via a connection string of the form
  `host=localhost port=5432 dbname=mydb` (see function `pg-connect/string`).
  
- Preliminary implementation of connection to PostgreSQL via a connection URI of the form
  `postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp&ssl=true` (see
  function `pg-connect/uri`).

- Add serialization function for the `bpchar` type.

- If the environment variable `PGAPPNAME` is set, it will override the default value of variable
  `pg-application-name`.


## [0.29] - 2024-03-02

- New function `pg-table-owner`.

- New functions `pg-table-comment` and `(setf pg-table-comment)`.

- New function `pg-column-default` which returns the default value for a column.


## [0.28] - 2024-02-21

- New functions `pg-escape-identifier` and `pg-escape-literal` to escape an SQL identifier (table,
  column or function name) or a string in an SQL command. These functions are similar respectively
  to libpq functions `PQescapeIdentifier` and `PQescapeLiteral`. These functions help to prevent SQL
  injection attacks. However, you should use prepared statements (elisp function `pg-exec-prepared`)
  instead of these functions whenever possible.


## [0.27] - 2024-01-10

- Improvements to the internal parsing functionality to use a hashtable instead of an alist to look
  up parsing functions (performance should be improved).

- Improved support for user-defined parsing for custom PostgreSQL types (see the function
  `pg-register-parser`).

- Add support for the pgvector extension for vector embeddings.


## [0.26] - 2023-12-18

- API change for `pg-fetch` when using prepared statements with a suspended portal. The second
  argument is a pgresult structure, rather than the portal name. This changes improves performance
  by making it possible to avoid redundant DescribeRow messages.

- The extended query flow used by `pg-exec-prepared` has been modified to be more asynchronous to
  improve performance: instead of waiting for a response after the Prepare and Bind phases, only
  wait a single time after the Execute phase.


## [0.25] - 2023-12-14

- Add support for the extended query syntax, which helps to avoid the risk of SQL injection attacks.
  See function `pg-exec-prepared`. This also makes it easier to fetch partial results from a query
  that returns a large number of rows.

- Fix parsing of PostgreSQL `CHAR` type to an Emacs character instead of a string of length 1.

- Various internal functions and variables renamed with the `pg--` prefix instead of `pg-`.



## [0.24] - 2023-11-15
### New
- Add function `pg-add-notification-handler` to add a function to the list of handlers for
  `NotificationResponse` messages from PostgreSQL. A handler takes two arguments, the channel and
  the payload, which correspond to SQL-level `NOTIFY channel, 'payload'`.

- Add support for asynchronous processing of messages from PostgreSQL, in particular for use of
  LISTEN/NOTIFY. This allows PostgreSQL and Emacs to be used in a publish-subscribe pattern which
  decouples event publication from the number and the speed of event processing nodes. See the
  notification-publisher.el and notification-subscriber.el tests for a basic example.

### Fixed
- Fix the implementation of `pg-tables` and `pg-columns` to use the information schema instead of
  historical alternative SQL queries.


## [0.23] - 2023-08-20
### New
- Preliminary support for the COPY protocol. See function `pg-copy-from-buffer`.


## [0.22] - 2023-07-16
### Fixed
- The backend can send NoticeResponse messages during connection startup, for example indicating a
  collation version mismatch between your database and the operating system.


## [0.21] - 2023-04-23
### Fixed
- Declare some autoloaded functions to avoid warning from the bytecode compiler.


## [0.20] - 2022-12-10
### Fixed
- Wait for further data from the network in `pg-read-chars` if the process buffer doesn't yet
  contain the necessary data (fix from swilsons).


## [0.19] - 2022-11-19
### New
- Add support for parsing the `BIT` and `VARBIT` datatypes.
- Add support for parsing ARRAY datatypes.
- Add support for parsing RANGE datatypes (integer and numerical).
- Add support for parsing HSTORE datatypes (see function `pg-hstore-setup` to prepare the database
  connection for use of the HSTORE datatype).
- Add function `pg-cancel` to request cancellation of the command currently being processed
  by the backend.

### Fixed
- Fix bug in handling of DataRow messages when zero columns returned by query.


## [0.18] - 2022-10-16
### New
- Add support for connecting to PostgreSQL over a local Unix socket.
- Add support for parsing the `BYTEA` datatype (binary strings). We assume that the PostgreSQL
  configuration variable `bytea_output` is set to `hex` (the default setting).
- Add support for parsing the `JSON` datatype, into the Emacs JSON representation.
- Add support for parsing the `JSONB` datatype, into the Emacs JSON representation.
- Add support for handling ParameterStatus messages sent by the backend (see variable
  `pg-parameter-change-functions`).
- Add support for handling NOTICE messages sent by the backend (see variable
  `pg-handle-notice-functions`).
- New pg-error and pg-protocol-error error types. All errors raised by the library will be a
  subclass of pg-error.

### Fixed
- Fix bug in parsing of NULL column values in DataRow messages.
- Fix handling of encoding of attribute column names.
- Fix handling of PostgreSQL error messages (correctly resync with the backend).


## [0.17] - 2022-09-30
### Updated
- Support for encrypted (TLS) connections with PostgreSQL
- Native support for PBKDF2 algorithm to allow SCRAM-SHA-256 authentication without the external
  nettle-pbkdf2 application
- Implement multibyte encoding and decoding for pg-exec requests
- Send application_name to PostgreSQL backend to improve observability
- Fix handling of NotificationResponse messages 
- Improve test coverage
- Include continuous integration tests on Windows and MacOS (GitHub actions)
- This version distributed via MELPA


## [0.16] - 2022-09-18
### Updated
- Fix MD5 authentication
- Use client-encoding to decode PostgreSQL error messages
- Improve GitHub Actions continuous integration workflow


## [0.15] - 2022-09-06
### Updated
- Moved from cl library to cl-lib
- pg: prefix for symbol names changed to pg- (Emacs Lisp coding conventions)
- Implemented version 3.0 of the PostgreSQL wire protocol
- Implemented SCRAM-SHA-256 authentication
- Implemented MD5 authentication
- Distributed via github repository



## [0.11] - 2001

This version was distributed from http://purl.org/net/emarsden/home/downloads/
and via the EmacsWiki. 
