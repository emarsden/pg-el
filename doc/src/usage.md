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
ELISP> (defvar *pg* (pg-connect-plist "pgeltestdb" "pgeltestuser" :password "pgeltest" :host "localhost"))
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

You can create and delete tables, and query the database metainformation using `pg-tables` and
`pg-columns`.

~~~admonish example title="Working with tables and DDL"
```lisp
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE messages(id BIGSERIAL PRIMARY KEY, msg TEXT)") :status)
"CREATE TABLE"
ELISP> (member "messages" (pg-tables *pg*))
("messages")
ELISP> (member "msg" (pg-columns *pg* "messages"))
("msg")
ELISP> (pg-result (pg-exec *pg* "DROP TABLE messages") :status)
"DROP TABLE"
ELISP> (member "messages" (pg-tables *pg*))
nil
```
~~~


The library has support for PostgreSQL's **extended query protocol** (prepared statements), which you
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

Boolean vectors are only supported in Emacs from version 27 onwards (you can check whether the
function `make-bool-vector` is fboundp).

```lisp
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

Emacs has support for bignums from version 27.2 onwards.

~~~admonish example title="Using bignums"
```lisp
ELISP> (fboundp 'bignump)
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


~~~admonish example title="Numerical ranges"
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


## Working with binary data

The [BYTEA type](https://www.postgresql.org/docs/current/datatype-binary.html) allows the storage of
binary strings, i.e. sequences of octets. They can contain NUL octets (the value zero).

~~~admonish example title="Using the BYTEA type"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT '\\xDEADBEEF'::bytea")))
         (equal (car (pg-result res :tuple 0))
                (decode-hex-string "DEADBEEF")))
t
ELISP> (let ((res (pg-exec *pg* "SELECT '\\001\\003\\005'::bytea")))
         (equal (car (pg-result res :tuple 0))
                (string 1 3 5)))
t
ELISP> (let ((res (pg-exec *pg* "SELECT '\\x123456'::bytea || '\\x789a00bcde'::bytea")))
         (equal (car (pg-result res :tuple 0))
                (decode-hex-string "123456789a00bcde")))
t
ELISP> (let ((res (pg-exec *pg* "SELECT 'warning\\000'::bytea")))
         (equal (length (car (pg-result res :tuple 0))) 8))
t

```
~~~

When sending binary data to PostgreSQL, either encode all potentially problematic octets, as we did
for NUL above, or send base64-encoded content and decode it in PostgreSQL. There are various other
useful functions for working with binary data on PostgreSQL, such as hash functions.
 

~~~admonish example title="Encoding and decoding binary data"
```lisp
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE bt(blob BYTEA, tag int)") :status)
"CREATE TABLE"
ELISP> (let* ((size 512)
              (random-octets (make-string size 0)))
         (dotimes (i size)
           (setf (aref random-octets i) (random 256)))
         (setf (aref random-octets 0) 0)
         (pg-exec-prepared *pg*
            "INSERT INTO bt VALUES (decode($1, 'base64'), 42)"
            `((,(base64-encode-string random-octets) . "text")))
         (equal random-octets (car (pg-result (pg-exec *pg* "SELECT blob FROM bt WHERE tag=42") :tuple 0))))
t
ELISP> (let* ((res (pg-exec *pg* "SELECT sha256('foobles'::bytea)"))
              (hx (encode-hex-string (car (pg-result res :tuple 0)))))
          (equal hx (secure-hash 'sha256 "foobles")))
t
ELISP> (let* ((res (pg-exec *pg* "SELECT md5('foobles')"))
              (r (car (pg-result res :tuple 0))))
          (equal r (md5 "foobles")))
t
ELISP> (let* ((res (pg-exec *pg* "SELECT encode('foobles', 'base64')"))
              (r (car (pg-result res :tuple 0))))
          (equal r (base64-encode-string "foobles")))
t
```
~~~



## PostgreSQL arrays

(To be documented)

