# Using prepared statements

pg-el has support for PostgreSQL's **extended query protocol** (prepared statements), which you
should use to prevent SQL injection attacks.

~~~admonish example title="Prepared statements and the extended query protocol"
```lisp
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE count_test(key INTEGER, val INTEGER)") :status)
"CREATE TABLE"
ELISP> (dotimes (i 100)
         (pg-exec-prepared *pg* "INSERT INTO count_test VALUES($1, $2)"
            `((,i . "int4") (,(* i i) . "int4"))))
nil
ELISP> (let ((res (pg-exec *pg* "SELECT count(*) FROM count_test")))
          (car (pg-result res :tuple 0)))
100
ELISP> (defvar *multires* (pg-exec-prepared *pg* "SELECT key FROM count_test" nil :max-rows 10))
*multires*
ELISP> (pg-result *multires* :tuples)
((0)
 (1)
 (2)
 (3)
 (4)
 (5)
 (6)
 (7)
 (8)
 (9))
ELISP> (pg-result *multires* :incomplete)
t
ELISP> (setq *multires* (pg-fetch *pg* *multires* :max-rows 5))
;; *multires*
ELISP> (pg-result *multires* :tuples)
((10)
 (11)
 (12)
 (13)
 (14))
ELISP> (pg-result *multires* :incomplete)
t
ELISP> (setq *multires* (pg-fetch *pg* *multires* :max-rows 100))
;; *multires*
ELISP> (length (pg-result *multires* :tuples))
85
ELISP> (pg-result *multires* :incomplete)
nil
```
~~~


If your application will use the same prepared statement multiple times, you can ask PostgreSQL to
parse/analyze the SQL query and bind parameters once, then use the prepared statement with different
variable values multiple times. This will improve performance by avoiding the overhead of reparsing
and reoptimizing a query plan multiple times.

~~~admonish example title="Fetching from a previously prepared statement"

The example function below (which comes from the [PGmacs](https://github.com/emarsden/pgmacs)
browsing/editing interface for PostgreSQL) illustrates the use of the utility function
`pg-ensure-prepared-statement`, which either retrieves the cached prepared statement if the function
has already been called (pg-el maintains a per-connection cache of prepared statements), or prepares
the statement given the SQL and the argument types if the function has not yet been called in this
PostgreSQL connection. The prepared statement is executed using `pg-fetch-prepared`, which functions
in a similar way to function `pg-fetch`.

```lisp
(defun pgmacs--table-primary-keys (con table)
  "Return the columns active as PRIMARY KEY in TABLE.
Uses PostgreSQL connection CON."
  (let* ((schema (if (pg-qualified-name-p table)
                     (pg-qualified-name-schema table)
                   "public"))
         (tname (if (pg-qualified-name-p table)
                    (pg-qualified-name-name table)
                  table))
         (sql "SELECT a.attname
               FROM pg_catalog.pg_index idx
               JOIN pg_catalog.pg_class c ON c.oid = idx.indrelid
               JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(idx.indkey)
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE relname = $1 AND nspname = $2 AND indisprimary")
         (argument-types (list "text" "text"))
         (params `((,tname . "text") (,schema . "text")))
         (ps-name (pg-ensure-prepared-statement con "QRY-tbl-primary-keys" sql argument-types))
         (res (pg-fetch-prepared con ps-name params)))
    (mapcar #'cl-first (pg-result res :tuples))))

```
~~~


