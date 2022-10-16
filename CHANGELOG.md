# Changelog

## [0.18] - 2022-10
### New
- Add support for connecting to PostgreSQL over a local Unix socket.
- Add support for parsing the `BYTEA` datatype (binary strings). We assume that the PostgreSQL
  configuration variable `bytea_output` is set to `hex` (the default setting).
- Add support for parsing the `JSON` datatype, into the Emacs JSON representation.
- Add support for handling ParameterStatus messages sent by the backend (see variable
  `pg-parameter-change-functions`).
- Add support for handling NOTICE messages sent by the backend (see variable
  `pg-handle-notice-functions`).
- New pg-error and pg-protocol-error error types. All errors raised by the library will be a
  subclass of pg-error.

### Fixed
- Fix bug in parsing of NULL column values in DataRow messages
- Fix handling of encoding of attribute column names
- Fix handling of PostgreSQL error messages (correctly resync with the backend)


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
