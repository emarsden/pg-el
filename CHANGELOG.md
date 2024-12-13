# Changelog


## [0.45] - Unreleased

- When supported by Emacs, enable `TCP_NODELAY` on the network connections to PostgreSQL to disable
  Nagle's algorithm (network segments are sent as soon as possible, even when they contain little
  data). This is done by libpq and results in a 12x (!) speedup on some benchmarks. This is a new
  feature available in (currently unreleased) Emacs 31 (bug#74793).


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
