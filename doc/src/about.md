# pg-el

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html) 
[![Latest tagged version](https://img.shields.io/github/v/tag/emarsden/pg-el?label=Latest%20tagged%20version)](https://github.com/emarsden/pg-el/)
[![NonGNU ELPA](https://elpa.nongnu.org/nongnu/pg.svg)](https://elpa.nongnu.org/nongnu/pg.html)
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

The code has been tested with **PostgreSQL versions** 17.5, 16.4, 15.4, 13.8, 11.17, and 10.22 on
Linux. It is also tested via GitHub actions on MacOS and Microsoft Windows. This library also works,
to a variable extent, against other databases that implement the PostgreSQL wire protocol; please
see our [README](https://github.com/emarsden/pg-el/blob/main/README.md) for the up to date list.

The generic function `pg-do-variant-specific-setup` allows you to specify setup operations to run
for a particular semi-compatible PostgreSQL variant. You can specialize it on the symbol name of the
variant, currently one of `postgresql`, `alloydb`, `cratedb`, `cockroachdb`, `yugabyte`, `questdb`,
`greptimedb`, `risingwave`, `immudb`, `timescaledb`, `ydb`, `orioledb`, `xata`, `spanner`,
`ivorydb`, `readyset`, `materialize`, `greenplum`, `clickhouse`, `octodb`, `vertica`, `arcadedb`,
`polardb`, `agensgraph`. As an example, the following specializer is already defined to run for
AlloyDB variants:

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




Tested with **Emacs versions** 31 pre-release, 30.1 and 29.4. Emacs versions older than 26.1 will
not work against a recent PostgreSQL version (whose default configuration requires SCRAM-SHA-256
authentication), because they don’t include the GnuTLS support which we use to calculate HMACs. They
may however work against a database set up to allow unauthenticated local connections. Emacs
versions before 28.1 will not support the extended query protocol, because the `bindat` package is
required. We mostly test with Emacs on Linux, but the library also works fine on Microsoft Windows
and MacOS.

You may be interested in an alternative library [emacs-libpq](https://github.com/anse1/emacs-libpq)
that enables access to PostgreSQL from Emacs by binding to the libpq library.



## Licence

pg-el is free software distributed under the terms of the GNU GPL v3 or later.
