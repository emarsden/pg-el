# Using pg-el

The examples below illustrate various features of pg-el in conjunction with PostgreSQL. A more
complete set of examples can be found in our [test suite](https://github.com/emarsden/pg-el/tree/main/test).

The examples all assume that you are using ielm as an Emacs Lisp shell (start with `M-x ielm`) and
that you have a connection to PostgreSQL:

~~~admonish example title="Connect to PostgreSQL from Emacs"
```lisp
ELISP> (require 'cl-lib)
cl-lib
ELISP> (require 'pg)
pg
ELISP> (defvar *pg* (pg-connect "pgeltestdb" "pgeltestuser" "pgeltest" "localhost" 5432))
*pg*
```
~~~

The library should in principle convert from any obscure Emacs encoding to the UTF-8 supported by
PostgreSQL. 

~~~admonish example title="Unicode support"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT 'était ' || 'là'")))
         (pg-result res :tuple 0))
("était là")
ELISP> (let ((res (pg-exec *pg* "select length('(╯°□°)╯︵ ┻━┻')")))
         (pg-result res :tuple 0))
(12)
ELISP> (let ((res (pg-exec *pg* "SELECT '😎'")))
         (pg-result res :tuple 0))
("😎")
```
~~~


~~~admonish example title="Working with tables and DDL"
```lisp
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE count_test(key integer, val integer)") :status)
"CREATE TABLE"
ELISP> (member "count_test" (pg-tables *pg*))
("count_test")
ELISP> (member "val" (pg-columns *pg* "count_test"))
("val")
ELISP> (cl-loop for i from 1 to 100
         for sql = (format "INSERT INTO count_test VALUES(%s, %s)" i (* i i))
         do (pg-exec *pg* sql))
nil
ELISP> (let ((res (pg-exec *pg* "SELECT count(*) FROM count_test")))
          (car (pg-result res :tuple 0)))
100
ELISP> (pg-result (pg-exec *pg* "DROP TABLE count_test") :status)
"DROP TABLE"
ELISP> (member "count_test" (pg-tables *pg*))
nil
```
~~~


~~~admonish example title="Casting SQL values to a specific type"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT pi()::int4")))
          (car (pg-result res :tuple 0)))
3
ELISP> (let ((res (pg-exec *pg* "SELECT 42::text")))
          (car (pg-result res :tuple 0)))
"42"
ELISP> (let ((res (pg-exec *pg* "SELECT '42'::smallint")))
          (car (pg-result res :tuple 0)))
42 (#o52, #x2a, ?*)
ELISP> (let ((res (pg-exec *pg* "SELECT 'PT3H4M42S'::interval")))
          (car (pg-result res :tuple 0)))
"03:04:42"
```
~~~


~~~admonish example title="Working with boolean vectors"

Boolean vectors are only supported in Emacs from version 27 onwards.

```
ELISP> (let ((res (pg-exec *pg* "SELECT '1010'::bit(4)")))
          (equal (car (pg-result res :tuple 0))
                 (coerce (vector t nil t nil) 'bool-vector)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT b'1001000'")))
          (equal (car (pg-result res :tuple 0))
                 (coerce (vector t nil nil t nil nil nil) 'bool-vector)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT '101111'::varbit(6)")))
          (equal (car (pg-result res :tuple 0))
                 (coerce (vector t nil t t t t) 'bool-vector)))
t
```
~~~


~~~admonish example title="Using bignums"
```lisp
ELISP> (fboundp 'bignump) ;; only supported from Emacs 27.2 onwards
t
ELISP> (let ((res (pg-exec *pg* "SELECT factorial(25)")))
          (car (pg-result res :tuple 0)))
15511210043330985984000000 (#o6324500606375411017360000000, #xcd4a0619fb0907bc00000)
```
~~~


~~~admonish example title="Special floating point syntax"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT 'Infinity'::float4")))
          (car (pg-result res :tuple 0)))
1.0e+INF
ELISP> (let ((res (pg-exec *pg* "SELECT '-Infinity'::float8")))
          (car (pg-result res :tuple 0)))
-1.0e+INF
ELISP> (let ((res (pg-exec *pg* "SELECT 'NaN'::float8")))
          (car (pg-result res :tuple 0)))
0.0e+NaN
```
~~~


~~~admonish title="Numerical ranges"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT int4range(10, 20)")))
         (car (pg-result res :tuple 0)))
(:range 91 10 41 20)
;; note that 91 is the character ?\[ and 41 is the character ?\)
ELISP> (let ((res (pg-exec *pg* "SELECT int4range(10, 20)")))
         (equal (car (pg-result res :tuple 0))
                (list :range ?\[ 10 ?\) 20)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT int4range(5,15) + int4range(10,20)")))
         (equal (car (pg-result res :tuple 0))
                (list :range ?\[ 5 ?\) 20)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT int8range(5,15) * int8range(10,20)")))
         (equal (car (pg-result res :tuple 0))
                (list :range ?\[ 10 ?\) 15)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT '(3,7)'::int4range")))
         (equal (car (pg-result res :tuple 0))
                (list :range ?\[ 4 ?\) 7)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT int8range(1, 14, '(]')")))
         (equal (car (pg-result res :tuple 0))
                (list :range ?\[ 2 ?\) 15)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT '[4,4)'::int4range")))
         (equal (car (pg-result res :tuple 0))
                (list :range)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT numrange(33.33, 66.66)")))
         (car (pg-result res :tuple 0)))
(:range 91 33.33 41 66.66)
ELISP> (let ((res (pg-exec *pg* "SELECT upper(numrange(-50.0, -40.0))")))
         (car (pg-result res :tuple 0)))
-40.0
ELISP> (let ((res (pg-exec *pg* "SELECT numrange(NULL, 2.2)")))
         (car (pg-result res :tuple 0)))
(:range 40 nil 41 2.2)
ELISP> (let ((res (pg-exec *pg* "SELECT numrange(NULL, NULL)")))
         (car (pg-result res :tuple 0)))
(:range 40 nil 41 nil)
```
~~~


## Collation

Case support in PostgreSQL (`lower()` and `upper()` functions) depend on the current [collation
rules](https://www.postgresql.org/docs/current/collation.html). A table has a default collation
which is specified at creation (with a default). To remove the dependency on the
table's collation, you can specify the desired collation explicitly.

Note that PostgreSQL can be compiled with or without support for libicu, as a complement to the
libc collation support.

~~~admonish example title="Using different collation rules"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔÖÉ' COLLATE \"fr_FR\")")))
         (car (pg-result res :tuple 0)))
"fôöé"
ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔ🐘💥bz' COLLATE \"fr_FR\")")))
         (car (pg-result res :tuple 0)))
"fô🐘💥bz"
ELISP> (pg-result (pg-exec *pg* "CREATE COLLATION IF NOT EXISTS \"french\" (provider = icu, locale = 'fr_FR')") :status)
"CREATE COLLATION"
ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔÖÉ' COLLATE \"french\")")))
         (car (pg-result res :tuple 0)))
"fôöé"
```
~~~


## The BYTEA type



## PostgreSQL arrays



## JSON and JSONB values



## HSTORE


## The COPY protocol


## PREPARE / EXECUTE


## Special pg-el features 

pg-parameter-change-functions


Error handling and condition-case

Handling NOTICEs

NOTIFY / LISTEN
