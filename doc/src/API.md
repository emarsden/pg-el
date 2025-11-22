# The pg-el API


The entry points in the pg-el library are documented below.


    (with-pg-connection-plist con (dbname user &key args) &body body)

A macro which opens a TCP network connection to database `DBNAME`, executes the `BODY` forms then
disconnects. See function `pg-connect-plist` for details of the connection arguments `ARGS`.


    (with-pg-connection-local con (path dbname user [password]) &body body)

A macro which opens a connection to database `DBNAME` over a local Unix socket at `PATH`, executes
the `BODY` forms then disconnects. See function `pg-connect-local` for details of the connection
arguments.


    (with-pg-transaction con &body body)

A macro which executes the `BODY` forms wrapped in an SQL transaction. `CON` is a connection to the
database. If an error occurs during the execution of the forms, a ROLLBACK instruction is executed.


    (pg-connect-plist dbname user &key password host port tls-options direct-tls server-variant protocol-version) -> con
    
Connect to the database `DBNAME` on `HOST` (defaults to localhost) at `PORT` (defaults to 5432) via
TCP/IP and authenticate as `USER` with `PASSWORD`. `PASSWORD` may be a string, or a zero-argument
lambda function which returns the password as a string (this makes it possible to use the
auth-source functionality in Emacs). This library currently supports SCRAM-SHA-256 authentication
(the default method from PostgreSQL version 14 onwards), MD5 authentication and cleartext password
authentication.

This function also sets the output date type to `ISO` and initializes our type parser tables.

If `tls-options` is non-NIL, attempt to establish an encrypted connection to PostgreSQL by
passing `tls-options` to Emacs function `gnutls-negotiate`. `tls-options` is a Common-Lisp style
argument list of the form

```lisp
(list :priority-string "NORMAL:-MD5" :trustfiles (list "/etc/company/RootCA.crt"))
```

To use client certificates to authenticate the TLS connection, use a value of `TLS-OPTIONS` of the
form

```lisp
`(list :keylist ((,key ,cert)))
```

where `key` is the filename of the client certificate private key and `cert` is the filename of the
client certificate. These are passed to GnuTLS.

If argument `direct-tls` is non-NIL, attempt to establish a “direct” TLS connection to PostgreSQL,
as supported since PostgreSQL version 18. This saves a few network packets during the establishment
of a network connection. This connection mode uses ALPN and requires ALPN support in your Emacs (in
testing as of Emacs 30).

If argument `server-variant` is non-NIL, force the detected value of `pgcon-server-variant` to the
specified value. This may be needed for some PostgreSQL variants that we are not able to identify
via their version string and startup options, but for which we need to implement workarounds (the
primary culprit is currently Clickhouse).

If `protocol-version` is non-NIL, it should be a `(major-version . minor-version)` cons representing
the version of the PostgreSQL wire protocol to use. Currently `major-version` should be 3 and
`minor-version` should be 0 or 2.



    (pg-connect-local path dbname user [password]) -> con

Initiate a connection with the PostgreSQL backend over local Unix socket `PATH`. Connect to the
database `DBNAME` with the username `USER`, providing `PASSWORD` if necessary. Returns a connection
to the database (as an opaque type). `PASSWORD` defaults to an empty string.



    (pg-connect/string connection-string) -> con

Connect to PostgreSQL with parameters specified by `CONNECTION-STRING`. A connection string is of
the form

    host=localhost port=5432 dbname=mydb options=--client_min_messages=DEBUG4

We do not support all the parameter keywords supported by libpq, such as those which specify
particular aspects of the TCP connection to PostgreSQL (e.g. `keepalives_interval`). The supported
keywords are `host`, `hostaddr`, `port`, `dbname`, `user`, `password`, `sslmode` (partial support),
`connect_timeout`, `read_timeout` (a pg-el extension), `client_encoding`, `application_name` and
`options`. They work in the same way as for libpq (and therefore for the psql commandline tool). The
same environment variable fallbacks as libpq are used (`PGHOST` for `host`, `PGOPTIONS` for options
and so on).


    (pg-connect/uri connection-uri) -> con

Connect to PostgreSQL with parameters specified by `CONNECTION-uri`. A connection URI is of
the form

    postgresql://[userspec@][hostspec][/dbname][?paramspec]

where `userspec` is of the form `username:password`. If `hostspec` is a string representing a local
path (e.g. `%2Fvar%2Flib%2Fpostgresql` with percent-encoding) then it is interpreted as a Unix
pathname used for a local Unix domain connection. We do not support all the paramspec keywords
supported by libpq, such as those which specify particular aspects of the TCP connection to
PostgreSQL (e.g. `keepalives_interval`). The supported paramspec keywords and their fallback
environment variables are the same as for pg-connect/string (see above).


    (pg-exec con &rest sql) -> pgresult

Concatenate the SQL strings and send to the PostgreSQL backend over connection `CON`. Retrieve the
information returned by the database and return it in an opaque record PGRESULT. The content of the
pgresult should be accessed using the `pg-result` function.


    (pg-exec-prepared con query typed-arguments &key (max-rows 0)) -> pgresult

Execute SQL query `QUERY`, which may include numbered parameters such as `$1`, ` $2` and so on,
using PostgreSQL's extended query protocol, on database connection `CON`. The `TYPED-ARGUMENTS` are
a list of the form 

    '((42 . "int4") ("42" . "text"))

This query will return at most `MAX-ROWS` rows (a value of zero indicates no limit). It returns a
pgresult structure (see function `pg-result`). This method is useful to reduce the risk of SQL
injection attacks.


     (pg-result pgresult what &rest args) -> info

Extract information from the `PGRESULT` returned by `pg-exec`. The `WHAT` keyword can be one of

* `:connection`: retrieve the database connection.

* `:status`: a string returned by the backend to indicate the status of the command; it is something
   like "SELECT" for a select command, "DELETE 1" if the deletion affected a single row, etc.

* `:attributes`: a list of tuples providing metadata: the first component of each tuple is the
   attribute's name as a string, the second an integer representing its PostgreSQL type, and the third
   an integer representing the size of that type.

* `:tuples`: all the data retrieved from the database, as a list of lists, each list corresponding
   to one row of data returned by the backend.

* `:tuple` tuple-number: return a specific tuple (numbering starts at 0).

* `:incomplete`: determine whether the set of tuples returned in this query set is incomplete, due
  to a suspended portal. If true, further tuples can be obtained by calling `pg-fetch`.

* `:oid`: allows you to retrieve the OID returned by the backend if the command was an insertion.
   The OID is a unique identifier for that row in the database (this is PostgreSQL-specific; please
   refer to the documentation for more details).
 
.

    (pg-fetch con result &key (max-rows 0))

Fetch pending rows from the suspended portal in `RESULT` on database connection `CON`.
This query will retrieve at most `MAX-ROWS` rows (default value of zero means no limit).
Returns a pgresult structure (see function `pg-result`). When used in multiple fetch situations
(with the `:max-rows` parameter to `pg-exec-prepared` which allows you to retrieve large result sets
incrementally), the same pgresult structure (initally returned by `pg-exec-prepared`) should be
passed to each successive call to `pg-fetch`, because it contains column metainformation that is
required to parse the incoming data. Each successive call to `pg-fetch` will return this pgresult
structure with new tuples accessible via `pg-result :tuples`. When no more tuples are available,
the `:incomplete` slot of the pgresult structure will be nil.


    (pg-cancel con) -> nil

Ask the server to cancel the command currently being processed by the backend. The cancellation
request concerns the command requested over database connection `CON`.


    (pg-disconnect con) -> nil

Close the database connection `CON`.


    (pg-for-each con select-form callback)

Calls `CALLBACK` on each tuple returned by `SELECT-FORM`. Declares a cursor for `SELECT-FORM`, then
fetches tuples using repeated executions of `FETCH 1`, until no results are left. The cursor is then
closed. The work is performed within a transaction. When you have a large amount of data to handle,
this usage is more efficient than fetching all the tuples in one go.

If you wish to browse the results, each one in a separate buffer, you could have the callback insert
each tuple into a buffer created with `(generate-new-buffer "myprefix")`, then use ibuffer's "/ n" to
list/visit/delete all buffers whose names match myprefix.


    (pg-databases con) -> list of strings

Return a list of the databases available over PostgreSQL connection `CON`. A database is a set of
tables; in a fresh PostgreSQL installation there is a single database named "template1".


    (pg-schemas con) -> list of strings

Return the list of the schemas in the PostgreSQL server to which we are connected with `CON`.
Schemas are a form of namespace, which can contain elements such as tables, sequences, indexes and
views. The default schema name is `public`.


    (pg-tables con) -> list of strings

Return a list of the tables present in the database to which we are currently connected over `CON`.
Only include user tables: system tables are not included in this list.


    (pg-columns con table) -> list of strings
    
Return a list of the columns (or attributes) in `TABLE`, which must be a table in the database to
which we are connected over `CON`. We only include the column names; if you want more detailed
information (attribute types, for example), it can be obtained from `pg-result` on a SELECT
statement for that table.


    (pg-table-comment con table) -> opt-string

Return the comment on `TABLE`, which must be a table in the database to which we are connected over
`CON`. Return nil if no comment is defined for `TABLE`. A setf function allows you to change the
table comment, or delete it with an argument of nil:

    (setf (pg-table-comment con "table") "The comment")


    (pg-column-comment con table column) -> opt-string

Return the comment on `COLUMN` in `TABLE` in a PostgreSQL database. `TABLE` can be a string or a
schema-qualified name. Uses database connection `CON`. Returns a string or nil if no comment is
defined. A setf function allows you to change the column comment, or delete it with a value of nil:

    (setf (pg-column-comment con "table" "column") "The comment")


    (pg-hstore-setup con)

Prepare for the use of HSTORE datatypes over database connection `CON`. This function must be called
before using the HSTORE extension. It loads the extension if necessary, and sets up the parsing
support for HSTORE datatypes.


    (pg-vector-setup con)
    
Prepare for the use of VECTOR datatypes from the pgvector extension over database connection `CON`.
This function must be called before using the pgvector extension. It loads the extension if
necessary, and sets up the parsing support for vector datatypes.




Variable `pg-parameter-change-functions` is a list of handlers to be called when the backend informs
us of a parameter change, for example a change to the session time zone. Each handler is called with
three arguments: the connection to the backend, the parameter name and the parameter value. It is
initially set to a function that looks out for `client_encoding` messages and updates the value
recorded in the connection.

Variable `pg-handle-notice-functions` is a list of handlers to be called when the backend sends us a
`NOTICE` message. Each handler is called with one argument, the notice, as a pgerror struct. 


Boolean variable `pg-disable-type-coercion` can be set to non-nil (before initiating a connection)
to disable the library's type coercion facility. Default is `t`.



~~~admonish warning title="Security note"

Setting up PostgreSQL to accept TCP/IP connections has security implications; please consult the
documentation for details. It is possible to use the port forwarding capabilities of ssh to
establish a connection to the backend over TCP/IP, which provides both a secure authentication
mechanism and encryption (and optionally compression) of data passing through the tunnel. Here's how
to do it (thanks to Gene Selkov, Jr. for the description):

1. Establish a tunnel to the backend machine, like this:

	ssh -L 3333:backend.dom:5432 postgres@backend.dom

   The first number in the -L argument, 3333, is the port number of your end of the tunnel. The
   second number, 5432, is the remote end of the tunnel -- the port number your backend is using.
   The name or the address in between the port numbers belongs to the server machine, as does the
   last argument to ssh that also includes the optional user name. Without the user name, ssh will
   try the name you are currently logged on as on the client machine. You can use any user name the
   server machine will accept, not necessarily those related to postgres.

2. Now that you have a running ssh session, you can point pg.el to the local host at the port number
   which you specified in step 1. For example,

        (pg-connect-plist "dbname" "user" :password "password" :host "localhost" :port 3333)

   You can omit the port argument if you chose 5432 as the local end of the tunnel, since pg.el
   defaults to this value.
~~~
