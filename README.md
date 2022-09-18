# pg.el -- Emacs Lisp socket-level interface to the PostgreSQL RDBMS

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0)
[![CI](https://github.com/emarsden/pg-el/workflows/check/badge.svg)](https://github.com/emarsden/pg-el/workflows/check/badge.svg)


This module lets you access the PostgreSQL object-relational DBMS from Emacs, using its socket-level
frontend/backend protocol. The module is capable of automatic type coercions from a range of SQL
types to the equivalent Emacs Lisp type. This is a low level API, and won't be useful to end users.

The code has been tested with PostgreSQL versions 15 beta, 13.8, 11.17, and 10.22. It also works
against other databases that implement the PostgreSQL wire protocol:

- [CockroachDB](https://github.com/cockroachdb/cockroach): tested with CockroachDB CCL v22.1.7. Note
  that this database does not implement the large object functionality.

- [CrateDB](https://crate.io/): tested with version 5.0.1.


## Installation

To install manually, place the file `pg.el` in a directory on your `load-path`, byte-compile it
(using for example `B` in dired) and add the following to your Emacs initialization file:

    (require 'pg)

SCRAM-SHA-256 authentication (which is the default authentication method since PostgreSQL version
14) is implemented by calling out to the `nettle-pbkdf2` application, which must be installed
(typically available in the `nettle-bin` package of your distribution).



## Entry points


    (with-pg-connection con (dbname user [password host port]) &body body)

A macro which opens a connection to database `DBNAME`, executes the `BODY` forms then disconnects. See
function `pg-connect` for details of the connection arguments.


    (with-pg-transaction con &body body)

A macro which executes the `BODY` forms wrapped in an SQL transaction. `CON` is a connection to the
database. If an error occurs during the execution of the forms, a ROLLBACK instruction is executed.


    (pg-connect dbname user [password host port]) -> connection
    
Connect to the database `DBNAME` on `HOST` (defaults to localhost) at `PORT` (defaults to 5432) via TCP/IP
and authenticate as `USER` with `PASSWORD`. This library currently supports SCRAM-SHA-256
authentication (the default method from PostgreSQL version 14 onwards), MD5 authentication and
cleartext password authentication. Connecting over TLS is not supported, due to apparent limitations
in the TLS negotiation support provided by the Emacs routines from GnuTLS. This function also sets the output
date type to 'ISO', and initializes our type parser tables.


    (pg-exec connection &rest sql) -> pgresult
    
Concatenate the SQL strings and send to the PostgreSQL backend. Retrieve the information returned by the
database and return it in an opaque record PGRESULT.


     (pg-result pgresult what &rest args) -> info

Extract information from the `PGRESULT` returned by `pg-exec`. The `WHAT` keyword can be
one of

- `:connection`: retrieve the database connection.

- `:status`: a string returned by the backend to indicate the status of the command; it is something
  like "SELECT" for a select command, "DELETE 1" if the deletion affected a single row, etc.

- `:attributes`: a list of tuples providing metadata: the first component of each tuple is the
  attribute's name as a string, the second an integer representing its PostgreSQL type, and the third
  an integer representing the size of that type.

- `:tuples`: all the data retrieved from the database, as a list of lists, each list
  corresponding to one row of data returned by the backend. 

- `:tuple` tuple-number: return a specific tuple (numbering starts at 0).

- `:oid`: allows you to retrieve the OID returned by the backend if the command was an insertion.
  The OID is a unique identifier for that row in the database (this is PostgreSQL-specific, please
  refer to the documentation for more details).


     (pg-disconnect con) -> nil
    
Close the database connection `CON`.



    (pg-for-each connection select-form callback)

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


    (pg-tables con) -> list of strings

Return a list of the tables present in the database to which we are currently connected over `CON`.
Only include user tables: system tables are not included in this list.


    (pg-columns con table) -> list of strings
    
Return a list of the columns (or attributes) in `TABLE`, which must be a table in the database to
which we are connected over `CON`. We only include the column names; if you want more detailed
information (attribute types, for example), it can be obtained from `pg-result` on a SELECT
statement for that table.


    (pg-lo-create con . args) -> oid

Create a new large object (BLOB, or binary large object in other DBMSes parlance) in the database to
which we are connected via `CON`. Returns an `OID` (which is represented as an elisp integer) which
will allow you to use the large object. Optional `ARGS` are a Unix-style mode string which determines
the permissions of the newly created large object, one of "r" for read-only permission, "w" for
write-only, "rw" for read+write. Default is "r".

Large-object functions MUST be used within a transaction (see the macro `with-pg-transaction`).


    (pg-lo-open con oid . args) -> fd

Open a large object whose unique identifier is `OID` (an elisp integer) in the database to which we
are connected via `CON`. Optional `ARGS` is a Unix-style mode string as for `pg-lo-create`; which
defaults to "r" read-only permissions. Returns a file descriptor (an elisp integer) which can be
used in other large-object functions.


    (pg-lo-close con fd)

Close the file descriptor `FD` which was associated with a large object. Note that this does not
delete the large object; use `pg-lo-unlink` for that.


    (pg-lo-read con fd bytes) -> string
    
Read `BYTES` from the file descriptor `FD` which is associated with a large object. Return an elisp
string which should be `BYTES` characters long.


    (pg-lo-write con fd buf)

Write the bytes contained in the elisp string `BUF` to the large object associated with the file
descriptor `FD`.


    (pg-lo-lseek cnn fd offset whence)

Do the equivalent of a `lseek(2)` on the file descriptor `FD` which is associated with a large
object; i.e. reposition the read/write file offset for that large object to `OFFSET` (an elisp
integer). `WHENCE` has the same significance as in `lseek()`; it should be one of `SEEK_SET` (set the
offset to the absolute position), `SEEK_CUR` (set the offset relative to the current offset) or
`SEEK_END` (set the offset relative to the end of the file). `WHENCE` should be an elisp integer whose
values can be obtained from the header file `<unistd.h>` (probably 0, 1 and 2 respectively).


    (pg-lo-tell con oid) -> integer
    
Do the equivalent of an `ftell(3)` on the file associated with the large object whose unique
identifier is `OID`. Returns the current position of the file offset for the object's associated file
descriptor, as an elisp integer.


    (pg-lo-unlink con oid)
    
Remove the large object whose unique identifier is `OID` from the system. In the current
implementation of large objects in PostgreSQL, each large object is associated with an object in the
filesystem.


    (pg-lo-import con filename) -> oid

Create a new large object and initialize it to the data contained in the file whose name is
`FILENAME`. Returns an `OID` (as an elisp integer). Note that this operation is only syntactic sugar
around the basic large-object operations listed above.


    (pg-lo-export conn oid filename)
    
Create a new file named `FILENAME` and fill it with the contents of the large object whose unique
identifier is `OID`. This operation is also syntactic sugar.


Boolean variable `pg-disable-type-coercion` can be set to non-nil (before initiating a connection)
to disable the library's type coercion facility. Default is `t`.


Please note that your PostgreSQL backend has to be started with the `-i` option in order to accept
TCP/IP connections (this is not the default). For more information about PostgreSQL see
<http://www.PostgreSQL.org/>.


**Security note**: setting up PostgreSQL to accept TCP/IP connections has security implications;
please consult the documentation for details. It is possible to use the port forwarding capabilities
of ssh to establish a connection to the backend over TCP/IP, which provides both a secure
authentication mechanism and encryption (and optionally compression) of data passing through the
tunnel. Here's how to do it (thanks to Gene Selkov, Jr. for the description):

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

        (pg-connect "dbname" "user" "password" "localhost" 3333)

   You can omit the port argument if you chose 5432 as the local end of the tunnel, since pg.el
   defaults to this value.




## Acknowledgements

Thanks to Eric Ludlam for discovering a bug in the date parsing routines, to Hartmut Pilch and
Yoshio Katayama for adding multibyte support, and to Doug McNaught and Pavel Janik for bug fixes.


The **latest version** of this package should be available from

    <https://github.com/emarsden/pg-el>
