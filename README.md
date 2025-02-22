# pg.el -- Emacs Lisp socket-level interface to the PostgreSQL RDBMS

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![MELPA](https://melpa.org/packages/pg-badge.svg)](https://melpa.org/#/pg)
[![test-pgv16](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)](https://github.com/emarsden/pg-el/actions/)
[![Documentation build](https://img.shields.io/github/actions/workflow/status/emarsden/pg-el/mdbook.yml?label=Documentation)](https://github.com/emarsden/pg-el/actions/)

This library lets you access the PostgreSQL 🐘 database management system from Emacs, using its
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


Tested **PostgreSQL versions**: The code has been tested with versions 17.4, 16.4, 15.4, 13.8,
11.17, and 10.22 on Linux. It is also tested via GitHub actions on MacOS and Windows. This library
also works, more or less, against other “PostgreSQL-compatible” databases. There are four main points
where this compatibility may be problematic: 

- Compatibility with the PostgreSQL wire protocol. This is the most basic form of compatibility.

- Compatibility with the PostgreSQL flavour of SQL, such as row expressions, non-standard functions
  such as `CHR`, data types such as `BIT`, `VARBIT`, `JSON` and `JSONB`, user-defined ENUMS and so
  on, functionality such as `LISTEN`. Some databases that claim to be “Postgres compatible” don’t
  even support foreign keys, views, triggers, sequences, tablespaces and temporary tables (looking
  at you, Amazon Aurora DSQL).

- Implementation of the system tables that are used by certain pg-el functions, to retrieve the list
  of tables in a database, the list of types, and so on.

- Establishing encrypted TLS connections to hosted services. Most PostgreSQL client libraries (in
  particular the official client library libpq) use OpenSSL for TLS support, whereas Emacs uses
  GnuTLS, and you may encounter incompatibilities.

The following PostgreSQL-compatible databases have been tested:

- [Neon](https://neon.tech/) “serverless PostgreSQL” works perfectly.

- [ParadeDB](https://www.paradedb.com/) version 0.13.1 works perfectly (it's really a PostgreSQL
  extension rather than a distinct database implementation).

- [IvorySQL](https://www.ivorysql.org/) version 4.2 works perfectly (this fork of PostgreSQL adds
  some features for compatibility with Oracle).

- The [Timescale DB](https://www.timescale.com/) extension for time series data works perfectly
  (last tested with version 2.16.1).

- The [CitusDB](https://github.com/citusdata/citus) extension for sharding PostgreSQL over multiple
  hosts works perfectly (last tested with Citus version 12.1.5, which is based on PostgreSQL 16.6).

- The [Microsoft DocumentDB](https://github.com/microsoft/documentdb) extension for MongoDB-like
  queries works perfectly (last tested 2025-02 with version 16.6). Note that this is not the same
  product as Amazon DocumentDB.

- The [Hydra Columnar](https://github.com/hydradatabase/columnar) extension for column-oriented
  storage and parallel queries works perfectly (last tested 2025-02).

- The [PgBouncer](https://www.pgbouncer.org/) connection pooler for PostgreSQL works fine (last
  tested with version 1.23 in the default session pooling mode).

- [Google AlloyDB Omni](https://cloud.google.com/alloydb/omni/docs/quickstart) is a proprietary fork
  of PostgreSQL with Google-developed extensions, including a columnar storage extension, adaptive
  autovacuum, and an index advisor. It works perfectly with pg-el as of 2025-02 (version that
  reports itself as "15.7").

- [Xata](https://xata.io/) “serverless PostgreSQL” has many limitations including lack of support
  for `CREATE DATABASE`, `CREATE COLLATION`, for XML processing, for temporary tables, for cursors,
  for `EXPLAIN`, for `CREATE EXTENSION`, for functions such as `pg_notify`.

- [YugabyteDB](https://yugabyte.com/): last tested on 2025-02 against version 2.25. Mostly working
  though the `pg_sequences` table is not implemented so certain tests fail. YugabyteDB does not have
  full compatibility with PostgreSQL SQL, and for example `GENERATED ALWAYS AS` columns are not
  supported, and `LISTEN` and `NOTIFY` are not supported. It does support certain extensions such as
  pgvector, however.

- The [RisingWave](https://github.com/risingwavelabs/risingwave) event streaming database is mostly
  working. It does not support `GENERATED ALWAYS AS IDENTITY` or `SERIAL` columns, nor `VACUUM
  ANALYZE`. Last tested 2025-02 with v2.2.0.

- [CrateDB](https://crate.io/): last tested 2025-02 with version 5.10.1. CrateDB does not support
  rows (e.g. `SELECT (1,2)`), does not support the `time`, `varbit`, `bytea`, `jsonb` and `hstore`
  types, does not handle a query which only contains an SQL comment, does not handle various
  PostgreSQL functions such as `factorial`, does not return a correct type OID for text columns in
  rows returned from a prepared statement, doesn't support Unicode identifiers, doesn't support the
  `COPY` protocol, doesn't support `TRUNCATE TABLE`.

- [CockroachDB](https://github.com/cockroachdb/cockroach): last tested 2025-02 with CockroachDB CCL
  v24.3. Note that this database does not implement the large object functionality, and its
  interpretation of SQL occasionally differs from that of PostgreSQL. Currently fails with an
  internal error on the SQL generated by our query for `pg-table-owner`, and fails on the boolean
  vector syntax b'1001000'.

- [QuestDB](https://questdb.io/): last tested 2025-02 with version 8.2.2. This has very limited
  PostgreSQL support, and does not support the `integer` type for example.

- [Google Spanner](https://cloud.google.com/spanner): tested with the Spanner emulator (that reports
  itself as `PostgreSQL 14.1`) and the PGAdapter library that enables support for the PostgreSQL
  wire protocol. Spanner has very limited PostgreSQL compatibility, for example refusing to create
  tables that do not have a primary key. It does not recognize basic PostgreSQL types such as `INT2`.
  It also does not for example support the `CHR` and `MD5` functions, row expressions, and WHERE
  clauses without a FROM clause.

- [YDB by Yandex](https://ydb.tech/docs/en/postgresql/docker-connect) last tested with version 23-4.
  Has very limited PostgreSQL compatibility. For example, an empty query string leads to a hung
  connection, and the `bit` type is returned as a string with the wrong oid.

- The [Materialize](https://materialize.com/) operational database (a proprietary differential
  dataflow database) has many limitations in its PostgreSQL compatibility: no support for primary
  keys, unique constraints, check constraints, for the 'bit' type for example. It works with these
  limitations with pg-el (last tested 2025-02 with Materialize v0.133).

- [GreptimeDB](https://github.com/GrepTimeTeam/greptimedb): this time series database implements
  quite a lot of the PostgreSQL wire protocol, but the names it uses for types in the
  `pg_catalog.pg_types` table are not the same as those used by PostgreSQL (e.g. `Int64` instead of
  `int8`), so our parsing machinery does not work. Last tested v0.12.0 in 2025-02.

- Hosted PostgreSQL services that have been tested: as of 2024-12
  [Railway.app](https://railway.app/) is running a Debian build of PostgreSQL 16.4, and works fine;
  [Aiven.io](https://aiven.io/) is running a Red Hat build of PostgreSQL 16.4 on Linux/Aarch64 and
  works fine.

- Untested but likely to work: Amazon RDS, Google Cloud SQL, Azure Database for PostgreSQL, Amazon
  Auroa. You may however encounter difficulties with TLS connections, as noted above.

It does not work with the ClickHouse database, whose PostgreSQL support is too limited. As of
version 25.1 in 2025-02, there is no implementation of the `pg_types` system table, no support for
basic PostgreSQL-flavoured SQL commands such as `SET`, no support for the extended query mechanism.


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

You can later update to the latest version with `M-x package-vc-upgrade RET pg RET`.




## Acknowledgements

Thanks to Eric Ludlam for discovering a bug in the date parsing routines, to Hartmut Pilch and
Yoshio Katayama for adding multibyte support, and to Doug McNaught and Pavel Janik for bug fixes.

