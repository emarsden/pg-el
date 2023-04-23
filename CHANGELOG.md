# Changelog

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
