# pg-el

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0)
[![MELPA](https://melpa.org/packages/pg-badge.svg)](https://melpa.org/#/pg)
[![test-pgv16](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)

This Emacs Lisp library lets you access the [PostgreSQL](https://www.postgresql.org/) 🐘
object-relational DBMS from Emacs, using its network-level frontend/backend protocol. The library is
capable of automatic type coercions from a range of SQL types to the equivalent Emacs Lisp type.

This libary will be useful for developers, rather than end users. If you're looking for an
Emacs-based browser/editor for PostgreSQL, you may be interested in
[PGmacs](https://github.com/emarsden/pgmacs/), which uses this library to communicate with
PostgreSQL or a compatible database.


~~~admonish note title="Supported features"

- SCRAM-SHA-256 authentication (the default method since PostgreSQL version 14) as well as MD5 and
  password authentication.

- Encrypted (TLS) connections between Emacs and the PostgreSQL backend.

- **Prepared statements** using PostgreSQL's extended query message flow, that allows for parameterized queries to
  protect from SQL injection issues.

- The PostgreSQL **COPY protocol** to copy preformatted data to PostgreSQL from an Emacs buffer.

- Asynchronous handling of LISTEN/NOTIFY notification messages from PostgreSQL, allowing the
  implementation of publish-subscribe type architectures (PostgreSQL as an “event broker” or
  “message bus” and Emacs as event publisher and consumer).

- Parsing various PostgreSQL types including integers, floats, array types, numerical ranges, JSON
  and JSONB objects into their native Emacs Lisp equivalents. The parsing support is
  user-extensible. Support for the HSTORE and pgvector extensions.

- Connections over TCP or (on Unix machines) a local Unix socket.
~~~

The code has been tested with **PostgreSQL versions** 17beta2, 16.3, 15.4, 13.8, 11.17, and 10.22 on
Linux. It is also tested via GitHub actions on MacOS and Microsoft Windows. This library also works,
to a variable extent, against other databases that implement the PostgreSQL wire protocol:

- [YugabyteDB](https://yugabyte.com/): tested against version 2.21. This database uses a lot of
  code from PostgreSQL 11 and is quite compatible, including with the HSTORE and pgvector
  extensions. However, some system tables differ from PostgreSQL, such as the `pg_sequences` table.
  It does not support the XML type. It does not support `LISTEN`/`NOTIFY`.

- [CrateDB](https://crate.io/): tested with version 5.7.2. There are limitations in this database's
  emulation of the PostgreSQL system tables: for example, it's not possible to query the owner of a
  table (function `pg-table-owner`). It doesn't accept SQL statements that only include an SQL
  comment. It doesn't support setting comments on SQL tables. As
  [documented](https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html), CrateDB
  does not support the `TIME` type without a time zone. It doesn't support casting integers to bits.
  It doesn't support the `VARBIT` type. It has no support for the COPY protocol.

- [CockroachDB](https://github.com/cockroachdb/cockroach): tested with CockroachDB CCL v24.1. Note
  that this database does not implement the large object functionality, and its interpretation of
  SQL occasionally differs from that of PostgreSQL. It is currently [reporting an internal
  error](https://github.com/cockroachdb/cockroach/issues/104009) when we call `pg-table-comment`.

- [ParadeDB](https://www.paradedb.com/): This ElasticSearch alternative is very
  PostgreSQL-compatible (more of an extension than a reimplementation). Tested with the Dockerhub
  instance which is based on PostgreSQL 16.3. All tests pass.

- [QuestDB](https://questdb.io/): tested against version 6.5.4. This is not very
  PostgreSQL-compatible: it fails on the SQL query `SELECT 1::integer` because it doesn't recognize
  integer as a type. It doesn't support `DELETE` statements.

- [ClickHouse](https://clickhouse.com/) doesn't work with pg-el. Their version 24.5 has a very basic
  implementation of the PostgreSQL wire protocol. It doesn't support the `pg_type` system table
  which provides information on the OIDs associated with different PostgreSQL types. All values are
  returned in textual format using the pseudo-OID of 0, which means the client must parse the value.
  The database immediately closes the connection on any SQL error. It doesn't support configuration
  statements such as `SET datestyle`. It doesn't specify a `server_name` in the startup sequence,
  which might allow us to detect this special case and restrict functionality to the most basic
  aspects.


Tested with **Emacs versions** 30-pre-release, 29.4, 28.2, 27.2 and 26.3. Emacs versions older than
26.1 will not work against a recent PostgreSQL version (whose default configuration requires
SCRAM-SHA-256 authentication), because they don’t include the GnuTLS support which we use to
calculate HMACs. They may however work against a database set up to allow unauthenticated local
connections. Emacs versions before 28.1 will not support the extended query protocol, because the
`bindat` package is required. We mostly test with Emacs on Linux, but the library also works fine on
Microsoft Windows and MacOS.

You may be interested in an alternative library [emacs-libpq](https://github.com/anse1/emacs-libpq)
that enables access to PostgreSQL from Emacs by binding to the libpq library.



## Licence

pg-el is free software distributed under the terms of the GNU GPL v2 or later.
