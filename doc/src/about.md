# pg-el

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0)
[![MELPA](https://melpa.org/packages/pg-badge.svg)](https://melpa.org/#/pg)
[![test-pgv16](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)](https://github.com/emarsden/pg-el/workflows/test-pgv16/badge.svg)

This Emacs Lisp library lets you access the [PostgreSQL](https://www.postgresql.org/) 🐘
object-relational DBMS from Emacs, using its socket-level frontend/backend protocol. The library is
capable of automatic type coercions from a range of SQL types to the equivalent Emacs Lisp type.


~~~admonish note title="Supported features"

- SCRAM-SHA-256 authentication (the default method since PostgreSQL version 14) as well as MD5 and
  password authentication.

- Encrypted (TLS) connections between Emacs and the PostgreSQL backend.

- Support for PostgreSQL's extended query syntax, that allows for parameterized queries to
  protect from SQL injection issues.

- Support for the `SQL COPY` protocol to copy preformatted data to PostgreSQL from an Emacs buffer

- Asynchronous handling of LISTEN/NOTIFY notification messages from PostgreSQL, allowing the
  implementation of publish-subscribe type architectures (PostgreSQL as an “event broker” or
  “message bus” and Emacs as event publisher and consumer).

- Parsing various PostgreSQL types including integers, floats, array types, numerical ranges, JSON
  and JSONB objects into their native Emacs Lisp equivalents. The parsing support is
  user-extensible. Support for the HSTORE and pgvector extensions.

- Connections over TCP or (on Unix machines) a local Unix socket.
~~~

The code has been tested with PostgreSQL versions 16.1, 15.4, 13.8, 11.17, and 10.22 on Linux. It is
also tested via GitHub actions on MacOS and Microsoft Windows, using the PostgreSQL version which is
pre-installed in the virtual images (currently 14.8). This library also works against other
databases that implement the PostgreSQL wire protocol:

- [YugabyteDB](https://yugabyte.com/): tested against version 2.19

- [CockroachDB](https://github.com/cockroachdb/cockroach): tested with CockroachDB CCL v23.1. Note
  that this database does not implement the large object functionality, and its interpretation of
  SQL occasionally differs from that of PostgreSQL.

- [CrateDB](https://crate.io/): tested with version 5.0.1.

- [QuestDB](https://questdb.io/): tested against version 6.5.4

Tested with Emacs versions 29.1, 28.2, 27.2 and 26.3. Emacs versions older than 26.1 will not work
against a recent PostgreSQL version (whose default configuration requires SCRAM-SHA-256
authentication), because they don’t include the GnuTLS support which we use to calculate HMACs. They
may however work against a database set up to allow unauthenticated local connections. Emacs
versions before 28.1 will not support the extended query protocol, because the bindat package is
required. We mostly test with Emacs on Linux, but the library also works fine on Microsoft Windows
and MacOS.

You may be interested in an alternative library [emacs-libpq](https://github.com/anse1/emacs-libpq)
that enables access to PostgreSQL from Emacs by binding to the libpq library.



## Licence

pg-el is free software distributed under the terms of the GNU GPL v2 or later.
