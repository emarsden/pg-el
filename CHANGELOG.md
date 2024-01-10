# Changelog


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
