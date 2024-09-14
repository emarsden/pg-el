# pg.el -- Emacs Lisp socket-level interface to the PostgreSQL RDBMS

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![MELPA](https://melpa.org/packages/pg-badge.svg)](https://melpa.org/#/pg)
[![test-pgv16](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)](https://github.com/emarsden/pg-el/actions/)
[![Documentation build](https://img.shields.io/github/actions/workflow/status/emarsden/pg-el/mdbook.yml?label=Documentation)](https://github.com/emarsden/pg-el/actions/)

This library lets you access the PostgreSQL 🐘 object-relational DBMS from Emacs, using its
network-level frontend/backend “wire” protocol. The module is capable of automatic type coercions from a
range of SQL types to the equivalent Emacs Lisp type.

📖 You may be interested in the [user manual](https://emarsden.github.io/pg-el/).

This is a developer-oriented library, which won’t be useful to end users. If you’re looking for a
browsing/editing interface to PostgreSQL in Emacs, you may be interested in
[PGmacs](https://github.com/emarsden/pgmacs/).

This library has support for:

- **SCRAM-SHA-256 authentication** (the default authentication method since PostgreSQL version 14),
  as well as MD5 and password authentication. There is currently no support for authentication
  using client-side certificates.

- Encrypted (**TLS**) connections with the PostgreSQL database, if your Emacs has been built with
  GnuTLS support. This includes support for authentication using client certificates.

- **Prepared statements** using PostgreSQL’s extended query protocol, to avoid SQL injection
  attacks.

- The PostgreSQL **COPY protocol** to copy preformatted data from an Emacs buffer to PostgreSQL, or
  to dump a PostgreSQL table or query result to an Emacs buffer in CSV or TSV format.

- Asynchronous handling of **LISTEN/NOTIFY** notification messages from PostgreSQL, allowing the
  implementation of **publish-subscribe architectures** (PostgreSQL as an “event broker” or
  “message bus” and Emacs as event publisher and consumer).

- Parsing various PostgreSQL types including integers, floats, array types, numerical ranges, JSON
  and JSONB objects into their native Emacs Lisp equivalents. The parsing support is
  user-extensible. Support for the HSTORE, pgvector and PostGIS extensions.

- Connections over TCP or (on Unix machines) a local Unix socket.


Tested **PostgreSQL versions**: The code has been tested with versions 17rc1, 16.4, 15.4, 13.8,
11.17, and 10.22 on Linux. It is also tested via GitHub actions on MacOS and Windows. This library
also works, more or less, against other “PostgreSQL-compatible” databases. There are four main points
where this compatibility may be problematic: 

- Compatibility with the PostgreSQL wire protocol. This is the most basic form of compatibility.

- Compatibility with the PostgreSQL flavour of SQL, such as row expressions, non-standard functions
  such as `CHR`, data types such as `BIT` and `VARBIT`, user-defined ENUMS and so on.

- Implementation of the system tables that are used by certain pg-el functions, to retrieve the list
  of tables in a database, the list of types, and so on.

- Establishing encrypted TLS connection to hosted services. Most PostgreSQL client libraries (in
  particular the official client library libpq) use OpenSSL for TLS support, whereas Emacs uses
  GnuTLS, and you may encounter incompatibilities.

The following PostgreSQL-compatible databases have been tested:

- [Neon](https://neon.tech/) “serverless PostgreSQL” works perfectly.

- [ParadeDB](https://www.paradedb.com/) version 0.9.1 works perfectly (it's really a PostgreSQL
  extension rather than a distinct database implementation).

- The [Timescale DB](https://www.timescale.com/) extension for time series data works perfectly
  (tested with version 2.16.1).

- [Xata](https://xata.io/) “serverless PostgreSQL” has many limitations including lack of support
  for `CREATE DATABASE`, `CREATE COLLATION`, for XML processing, for temporary tables, for cursors,
  for `EXPLAIN`, for `CREATE EXTENSION`, for functions such as `pg_notify`.

- [YugabyteDB](https://yugabyte.com/): tested against version 2.21, mostly working though the
  `pg_sequences` table is not implemented so certain tests fail. YugabyteDB does not have full
  compatibility with PostgreSQL SQL, and for example `GENERATED ALWAYS AS` columns are not
  supported, and `LISTEN` and `NOTIFY` are not supported.

- [CrateDB](https://crate.io/): tested with version 5.8.2. CrateDB does not support rows (e.g.
  `SELECT (1,2)`), does not support the `time` and `varbit` types, does not handle a query which
  only contains an SQL comment, does not handle various PostgreSQL functions such as `factorial`,
  and does not return a correct type OID for text columns in rows returned from a prepared statement.

- [CockroachDB](https://github.com/cockroachdb/cockroach): tested with CockroachDB CCL v24.1.3. Note
  that this database does not implement the large object functionality, and its interpretation of
  SQL occasionally differs from that of PostgreSQL. Currently fails with an internal error on the
  SQL generated by our query for `pg-table-owner`, and fails on the boolean vector syntax
  b'1001000'.

- [QuestDB](https://questdb.io/): tested against version 6.5.4. This has very limited PostgreSQL
  support, and does not support the `integer` type for example.

- [Google Spanner](https://cloud.google.com/spanner): tested with the Spanner emulator (that reports
  itself as `PostgreSQL 14.1`) and the PGAdapter library that enables support for the PostgreSQL
  wire protocol. Spanner has very limited PostgreSQL compatibility, for example refusing to create
  tables that do not have a primary key. It does not recognize basic PostgreSQL types such as INT2.
  It also does not for example support the `CHR` and `MD5` functions, row expressions, and WHERE
  clauses without a FROM clause.

- [YDB by Yandex](https://ydb.tech/docs/en/postgresql/docker-connect) version 23-4 has very limited
  PostgreSQL compatibility. For example, an empty query string leads to a hung connection, and the
  system tables that we query to obtain the list of tables in the current database are not
  implemented.

- Untested but likely to work: Amazon RDS, Google Cloud SQL, Azure Database for PostgreSQL, Amazon
  Auroa, Google AlloyDB, Materialize, CitusData. You may however encounter difficulties with TLS
  connections, as noted above.

It does not work with the ClickHouse database, whose PostgreSQL support is too limited (no
implementation of the `pg_types` system table, no support for basic SQL commands such as `SET`).

Tested **Emacs versions**: Tested with versions 30-prerelease, 29.4, 28.2, 27.2 and 26.3. Emacs
versions older than 26.1 will not work against a recent PostgreSQL version (whose default
configuration requires SCRAM-SHA-256 authentication), because they don’t include the GnuTLS support
which we use to calculate HMACs. They may however work against a database set up to allow
unauthenticated local connections. Emacs versions older than 28.1 (from April 2022) will not be able
to use the extended query protocol (prepared statements), because they don’t have the necessary
bindat functionality. It should however be easy to update the installed version of bindat.el for
these older versions.

You may be interested in an alternative library [emacs-libpq](https://github.com/anse1/emacs-libpq)
that enables access to PostgreSQL from Emacs by binding to the libpq library.


## Installation

Install via the [MELPA package archive](https://melpa.org/partials/getting-started.html) by
including the following in your Emacs initialization file (`.emacs.el` or `init.el`):

    (require 'package)
    (add-to-list 'package-archives '("melpa" . "https://melpa.org/packages/") t)

then saying 

     M-x package-install RET pg

Alternatively, you can install the library from the latest GitHub revision using:

     (unless (package-installed-p 'pg)
        (package-vc-install "https://github.com/emarsden/pg-el" nil nil 'pg))

You can later update these to the latest version with `M-x package-vc-upgrade RET pg RET`.




## Acknowledgements

Thanks to Eric Ludlam for discovering a bug in the date parsing routines, to Hartmut Pilch and
Yoshio Katayama for adding multibyte support, and to Doug McNaught and Pavel Janik for bug fixes.

