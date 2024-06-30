# Schema-qualified names


## Introduction to PostgreSQL schemas

A [schema in PostgreSQL](https://www.postgresql.org/docs/current/ddl-schemas.html) (and in the ANSI
SQL standard) is a collection of tables, views, functions, constraints, indexes and sequences.
PostgreSQL allows you to define multiple schemas in the same database, and different schemas can
include datastructres (such as tables) with the same name. You can think of them as namespaces for
tables. 

Here is the hierarchy of names:

- each PostgreSQL instance can have multiple databases;
- each database can contain multiple schemas;
- each schema can contain multiple tables (and views, functions and so on). 

In SQL syntax you will use **qualified names** including a schema, such as `myschema.mytable`
anywhere were you use a “normal” unqualified name `mytable`. Default objects created by the user are
in the schema named `public` (this schema is not normally printed in query results because the
current [`search_path`](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH)
includes `public`). Objects used internally by PostgreSQL are in system-defined schemas
such as `pg_catalog`, `pg_toast` and `information_schema`. 

The pg-el library represents schema-qualified names using `pg-qualified-name` objects, which are
`cl-defstruct` objects. This means you can create them as follows:

```lisp
(make-pg-qualified-name :schema "myschema" :name "mytable")
```

You can then use these objects anywhere you would use a normal table name, escaping special
characters using `pg-print-qualified-name`. The `pg-tables` function will return normal string names
for tables in the `public` namespace, and `pg-qualified-name` objects for tables in other namespaces.



~~~admonish example title="Using schema-qualified names"
```lisp
ELISP> (let ((res (pg-exec *pg* "SHOW search_path")))
         (pg-result res :tuple 0))
("\"$user\", public")
ELISP> (let ((res (pg-exec *pg* "CREATE SCHEMA custom")))
         (pg-result res :status))
"CREATE SCHEMA"
ELISP> (let* ((qn (make-pg-qualified-name :schema "custom" :name "mytable"))
              (sql (format "CREATE TABLE IF NOT EXISTS %s(id INTEGER)"
                           (pg-print-qualified-name qn)))
              (res (pg-exec *pg* sql)))
         (pg-result res :status))
"CREATE TABLE"
ELISP> (pg-tables *pg*)
("purchases" "customers" #s(pg-qualified-name :schema "custom" :name "mytable"))
;; We can use schema-qualified names as parameters for a prepared query.
ELISP> (let* ((qn (make-pg-qualified-name :schema "custom" :name "mytable"))
              (pqn (pg-print-qualified-name qn))
              (sql "SELECT pg_total_relation_size($1)")
              (res (pg-exec-prepared *pg* sql `((,pqn . "text")))))
         (pg-result res :tuple 0))
(0)
```

~~~
