# Interacting with large objects

The pg-el library includes support for PostgreSQL's [large object
functionality](https://www.postgresql.org/docs/current/largeobjects.html). This provides
stream-style access to data stored in file-like structures. It allows you to work with data values
that do not fit into RAM, by seeking to a particular file position then reading a subset of the full
file contents.

This functionality builds on the 64-bit functions in PostgreSQL's large object API (`lo_lseek64` and
friends), available from PostgreSQL version 9.3, and can therefore handle large objects of sizes up
to 4TB.

The following functions are available:

     (pg-lo-create con &optional mode)

Create a new large object and return its OID (represented in elisp by an integer). The mode argument
is ignored in PostgreSQL releases after v8.1.

Large-object functions MUST be used within a transaction (see the macro `with-pg-transaction`).


    (pg-lo-open con oid &optional mode)

Open the large object identified by `oid` for reading. Returns a large object descriptor (which works
similarly to a Unix file descriptor).


    (pg-lo-close con fd)

Close the large object described by `fd`. Note that this does not delete the large object; use
`pg-lo-unlink` for that.


    (pg-lo-read con fd bytes)
    
Read `bytes` octets from the large object described by `fd`. The value is returned as an Emacs Lisp
string.


    (pg-lo-write con fd buf)
    
Write the contents of the elisp string `buf` to the large object described by `fd`.


    (pg-lo-lseek con fd offset whence)
    
Seek to position `offset` in the large object designated by `fd`. `whence` can be `pg-SEEK_SET`
(seek from object start), `pg-SEEK_CUR` (seek from current position), or `pg-SEEK_END` (seek from
object end). `offset` may be a large integer (`int8` type in PostgreSQL; this function calls the
PostgreSQL backend function `lo_lseek64`). This function works in the same was as `lseek(2)` in Unix.


    (pg-lo-tell con fd)
    
Return the current file position in the large object designated by `fd`. This function works in the
same was as `ftell(3)` in Unix.


    (pg-lo-truncate con fd len)
    
Truncate the large object desginated by `fd` to size `len` (in octets).


    (pg-lo-unlink con oid)
    
Unlink (remove from the filesystem) the large object identified by `oid`.


    (pg-lo-import con filename)
    
Import the contents of `filename` (which must be accessible by the PostgreSQL backend on the server
host) as a large object. Returns the oid of the new object.


    (pg-lo-export con oid filename)
    
Export the content of the large object identified by `oid` to `filename` (a file path which must be
writable by the PostgreSQL backend on the server host).
