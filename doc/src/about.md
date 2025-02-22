# pg-el

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html) 
[![Latest tagged version](https://img.shields.io/github/v/tag/emarsden/pg-el?label=Latest%20tagged%20version)](https://github.com/emarsden/pg-el/)
[![MELPA](https://melpa.org/packages/pg-badge.svg)](https://melpa.org/#/pg) 
[![test-pgv16](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)

This Emacs Lisp library lets you access the [PostgreSQL](https://www.postgresql.org/) 🐘 database
from Emacs, using its network-level frontend/backend protocol. The library is capable of automatic
type coercions from a range of SQL types to and from the equivalent Emacs Lisp type.

This libary will be useful for developers, rather than end users. If you’re looking for an
Emacs-based browser/editor for PostgreSQL, you may be interested in
[PGmacs](https://github.com/emarsden/pgmacs/), which uses this library to communicate with
PostgreSQL or a compatible database.


~~~admonish note title="Supported features"

- SCRAM-SHA-256 authentication (the default method since PostgreSQL version 14) as well as MD5 and
  password authentication.

- Encrypted (TLS) connections between Emacs and the PostgreSQL backend. This includes support for
  client certificates.

- **Prepared statements** using PostgreSQL's extended query message flow, that allows for 
  parameterized queries to protect from SQL injection issues.

- The PostgreSQL **COPY protocol** to copy preformatted data to PostgreSQL from an Emacs buffer.

- Asynchronous handling of LISTEN/NOTIFY notification messages from PostgreSQL, allowing the
  implementation of publish-subscribe type architectures (PostgreSQL as an “event broker” or
  “message bus” and Emacs as event publisher and consumer).

- Parsing various PostgreSQL types including integers, floats, array types, numerical ranges, 
  JSON and JSONB objects into their native Emacs Lisp equivalents. The parsing support is
  user-extensible. Support for the HSTORE, pgvector and PostGIS extensions.

- Connections over TCP or (on Unix machines) a local Unix socket.
~~~

The code has been tested with **PostgreSQL versions** 17.4, 16.3, 15.4, 13.8, 11.17, and 10.22 on
Linux. It is also tested via GitHub actions on MacOS and Microsoft Windows. This library also works,
to a variable extent, against other databases that implement the PostgreSQL wire protocol:

- [Neon](https://neon.tech/) “serverless PostgreSQL” works perfectly.

- [ParadeDB](https://www.paradedb.com/) version 0.9.1 works perfectly (it's really a PostgreSQL
  extension rather than a distinct database implementation).

- [IvorySQL](https://www.ivorysql.org/) version 3.4 works perfectly (this fork of PostgreSQL adds
  some features for compatibility with Oracle).

- The [CitusDB](https://github.com/citusdata/citus) extension for sharding PostgreSQL over multiple
  hosts works perfectly (last tested with Citus version 12.1.5, which is based on PostgreSQL 16.6).

- The [Microsoft DocumentDB](https://github.com/microsoft/documentdb) extension for MongoDB-like
  queries works perfectly (last tested 2025-02 with version 16.6). Note that this is not the same
  product as Amazon DocumentDB.

- The [Hydra Columnar](https://github.com/hydradatabase/columnar) extension for column-oriented
  storage and parallel queries works perfectly (last tested 2025-02).

- The [Timescale DB](https://www.timescale.com/) extension for time series data works perfectly
  (tested with version 2.16.1).

- The [PgBouncer](https://www.pgbouncer.org/) connection pooler for PostgreSQL works fine (tested
  with version 1.23 in the default session pooling mode).

- [Google AlloyDB Omni](https://cloud.google.com/alloydb/omni/docs/quickstart) is a proprietary fork
  of PostgreSQL with Google-developed extensions, including a columnar storage extension, adaptive
  autovacuum, and an index advisor. It works perfectly with pg-el as of 2025-01.

- [ParadeDB](https://www.paradedb.com/): This ElasticSearch alternative is very
  PostgreSQL-compatible (more of an extension than a reimplementation). Tested with the Dockerhub
  instance which is based on PostgreSQL 16.3. All tests pass.

- [Xata](https://xata.io/) “serverless PostgreSQL” has many limitations including lack of support
  for `CREATE DATABASE`, `CREATE COLLATION`, for XML processing, for temporary tables, for cursors,
  for `EXPLAIN`, for `CREATE EXTENSION`, for functions such as `pg_notify`.

- [YugabyteDB](https://yugabyte.com/): tested against version 2.23. This database uses a lot of
  code from PostgreSQL 11 and is quite compatible, including with the HSTORE and pgvector
  extensions. However, some system tables differ from PostgreSQL, such as the `pg_sequences` table.
  It does not support the XML type. It does not support `LISTEN`/`NOTIFY`.

- The [RisingWave](https://github.com/risingwavelabs/risingwave) event streaming database is mostly
  working. It does not support `GENERATED ALWAYS AS IDENTITY` columns. Last tested 2025-02 with v2.1.2.

- [CrateDB](https://crate.io/): last tested 2025-02 with version 5.9.9. There are limitations in
  this database's emulation of the PostgreSQL system tables: for example, it's not possible to query
  the owner of a table (function `pg-table-owner`). It doesn't accept SQL statements that only
  include an SQL comment. It doesn't support setting comments on SQL tables. As
  [documented](https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html), CrateDB
  does not support the `TIME` type without a time zone. It doesn't support casting integers to bits.
  It doesn't support the `VARBIT` type. It has no support for the COPY protocol.

- [CockroachDB](https://github.com/cockroachdb/cockroach): tested with CockroachDB CCL v24.3. Note
  that this database does not implement the large object functionality, and its interpretation of
  SQL occasionally differs from that of PostgreSQL. It is currently [reporting an internal
  error](https://github.com/cockroachdb/cockroach/issues/104009) when we call `pg-table-comment`.

- [QuestDB](https://questdb.io/): tested against version 6.5.4. This is not very
  PostgreSQL-compatible: it fails on the SQL query `SELECT 1::integer` because it doesn't recognize
  integer as a type. It doesn't support `DELETE` statements.

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
  keys, unique constraints, check constraints, for the `bit` type for example. It works with these
  limitations with pg-el (last tested 2025-02 with Materialize v0.133).

- [ClickHouse](https://clickhouse.com/) doesn't work with pg-el. Their version 24.5 has a very basic
  implementation of the PostgreSQL wire protocol. It doesn’t support the `pg_type` system table
  which provides information on the OIDs associated with different PostgreSQL types. All values are
  returned in textual format using the pseudo-OID of 0, which means the client must parse the value.
  The database immediately closes the connection on any SQL error. It doesn't support configuration
  statements such as `SET datestyle`. It doesn't specify a `server_name` in the startup sequence,
  which might allow us to detect this special case and restrict functionality to the most basic
  aspects.

- [GreptimeDB](https://github.com/GrepTimeTeam/greptimedb) version 0.9.5 implements quite a lot of
  the PostgreSQL wire protocol, but the names it uses for types in the `pg_catalog.pg_types` table
  are not the same as those used by PostgreSQL (e.g. `Int64` instead of `int8`), so our parsing
  machinery does not work.

- Untested but likely to work: Amazon RDS, Google Cloud SQL, Azure Database for PostgreSQL, Amazon
  Auroa. You may however encounter difficulties with TLS connections, as noted above.

The generic function `pg-do-variant-specific-setup` allows you to specify setup operations to
run for a particular semi-compatible PostgreSQL variant. You can specialize it on the symbol name of the
variant, currently one of `postgresql`, `alloydb`, `cratedb`, `cockroachdb`, `yugabyte`, `questdb`,
`greptimedb`, `risingwave`, `immudb`, `timescaledb`, `ydb`, `orioledb`, `xata`, `spanner`,
`ivorydb`. As an example, the following specializer is already defined to run for AlloyDB variants:

```lisp
;; Register the OIDs associated with these OmniDB-specific types, so that their types appear in
;; column metadata listings.
(cl-defmethod pg-do-variant-specific-setup ((con pgcon) (_variant (eql 'alloydb)))
  (message "pg-el: running variant-specific setup for AlloyDB Omni")
  ;; These type names are in the google_ml schema
  (pg-register-parser "model_family_type" #'pg-text-parser)
  (pg-register-parser "model_family_info" #'pg-text-parser)
  (pg-register-parser "model_provider" #'pg-text-parser)
  (pg-register-parser "model_type" #'pg-text-parser)
  (pg-register-parser "auth_type" #'pg-text-parser)
  (pg-register-parser "auth_info" #'pg-text-parser)
  (pg-register-parser "models" #'pg-text-parser)
  (pg-initialize-parsers con))
```




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

pg-el is free software distributed under the terms of the GNU GPL v3 or later.
