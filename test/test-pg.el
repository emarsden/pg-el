;;; Tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2022-2024  Eric Marsden


;; pgeltestdb=> CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
;; CREATE TYPE
;; pgeltestdb=> CREATE TABLE person (
;;                                   name text,
;;                                   current_mood mood
;;                                   );
;; CREATE TABLE
;; pgeltestdb=> INSERT INTO person VALUES ('Moe', 'happy');
;; INSERT 0 1
;; pgeltestdb=> INSERT INTO person VALUES ('John', 'sad');
;; INSERT 0 1

;; FIXME need to extend pg--lookup-type-name so that when we query with an oid which is not in our
;; cache, we requery PG for new OIDs, associated for example with a newly-defined enum

(require 'cl-lib)
(require 'pg)
(require 'ert)


;; for performance testing
;; (setq process-adaptive-read-buffering nil)


(defmacro with-pgtest-connection (con &rest body)
  (cond ((getenv "PGURI")
         `(let ((,con (pg-connect/uri ,(getenv "PGURI"))))
            (unwind-protect
                (progn ,@body)
              (when ,con (pg-disconnect ,con)))))
        (t
         (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
               (user (or (getenv "PGEL_USER") "pgeltestuser"))
               (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
               (host (or (getenv "PGEL_HOSTNAME") "localhost"))
               (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
           `(with-pg-connection ,con (,db ,user ,password ,host ,port)
               ,@body)))))
(put 'with-pgtest-connection 'lisp-indent-function 'defun)

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (con &rest body)
  (cond ((getenv "PGURI")
         `(let ((,con (pg-connect/uri ,(getenv "PGURI"))))
            (unwind-protect
                (progn ,@body)
              (when ,con (pg-disconnect ,con)))))
        (t
         (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
               (user (or (getenv "PGEL_USER") "pgeltestuser"))
               (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
               (host (or (getenv "PGEL_HOSTNAME") "localhost"))
               (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
           `(with-pg-connection ,con (,db ,user ,password ,host ,port t)
               ,@body)))))
(put 'with-pgtest-connection-tls 'lisp-indent-function 'defun)

(defmacro with-pgtest-connection-local (con &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
    `(with-pg-connection-local ,con (,path ,db ,user ,password)
        ,@body)))
(put 'with-pg-connection-local 'lisp-indent-function 'defun)

(defun pg-connection-tests ()
  (dolist (v (list "host=localhost port=5432 dbname=pgeltestdb user=pgeltestuser password=pgeltest"
                   "port=5432 dbname=pgeltestdb user=pgeltestuser password=pgeltest"
                   "user=pgeltestuser sslmode=require port=5432 password=pgeltest dbname=pgeltestdb"))
    (let ((con (pg-connect/string v)))
      (should (process-live-p (pgcon-process con)))
      (pg-disconnect con)))
  (dolist (v (list "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb?application_name=testingtesting"
                   "postgres://pgeltestuser:pgeltest@localhost/pgeltestdb?application_name=testingtesting"
                   "postgres://pgeltestuser:pgeltest@localhost:5432/pgeltestdb"
                   "postgres://pgeltestuser:pgeltest@localhost:5432/pgeltestdb?sslmode=prefer"
                   "postgres://pgeltestuser:pgeltest@%2Fvar%2Frun%2Fpostgresql%2F.s.PGSQL.5432/pgeltestdb"))
    (let ((con (pg-connect/uri v)))
      (should (process-live-p (pgcon-process con)))
      (pg-disconnect con))))

(defun pg-run-tests (con)
  (pg-enable-query-log con)
  (message "Backend major-version is %s" (pgcon-server-version-major con))
  (message "Testing basic type parsing")
  (pg-test-basic con)
  (message "Testing insertions...")
  (pg-test-insert con)
  (message "Testing date routines...")
  (pg-test-date con)
  (message "Testing numeric routines...")
  (pg-test-numeric con)
  (when (>= emacs-major-version 28)
    (message "Testing prepared statements")
    (pg-test-prepared con)
    (pg-test-prepared/multifetch con)
    (pg-test-insert/prepared con))
  (pg-test-collation con)
  (pg-test-xml con)
  (pg-test-uuid con)
  (message "Testing field extraction routines...")
  (pg-test-result con)
  (pg-test-bytea con)
  (pg-test-sequence con)
  (pg-test-array con)
  (pg-test-schemas con)
  (pg-test-json con)
  (pg-test-server-prepare con)
  (pg-test-hstore con)
  (pg-test-vector con)
  (pg-test-tsvector con)
  (message "Testing COPY...")
  (pg-test-copy con)
  (pg-test-copy-large con)
  (message "Testing database creation")
  (pg-test-createdb con)
  (message "Testing unicode names for database, tables, columns")
  (pg-test-unicode-names con)
  (pg-test-returning con)
  (pg-test-parameter-change-handlers con)
  (message "Testing error handling")
  (pg-test-errors con)
  (pg-test-notice con)
  (pg-test-notify con)
  ;; (message "Testing large-object routines...")
  ;; (pg-test-lo-read)
  ;; (pg-test-lo-import)
  (message "Tests passed"))


(defun pg-test ()
  (cl-flet ((log-param-change (_con name value)
               (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection con
         (message "Running pg.el tests in %s against backend %s"
                  (version) (pg-backend-version con))
         (pg-run-tests con)))))

(defun pg-test-tls ()
  (cl-flet ((log-param-change (_con name value)
                              (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection-tls con
         (message "Running pg.el tests in %s against backend %s"
                  (version) (pg-backend-version con))
         (pg-run-tests con)))))

;; Run tests over local Unix socket connection to backend
(defun pg-test-local ()
  (cl-flet ((log-param-change (_con name value)
               (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection-local conn
         (message "Running pg.el tests in %s against backend %s"
                  (version) (pg-backend-version conn))
         (pg-run-tests conn)))))


(defun pg-test-prepared (con)
  (cl-labels ((row (query args) (pg-result (pg-exec-prepared con query args) :tuple 0))
              (scalar (query args) (car (row query args)))
              (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (should (equal 42 (scalar "SELECT 42" (list))))
    (should (approx= 42.0 (scalar "SELECT 42.00" (list))))
    (should (equal nil (scalar "SELECT NULL" (list))))
    (should (equal nil (scalar "" (list))))
    (should (equal (make-bool-vector 1 nil) (scalar "SELECT $1::bit" '(("0" . "bit")))))
    (should (equal (make-bool-vector 1 nil) (scalar "SELECT $1" '(("0" . "bit")))))
    (should (equal (bool-vector t nil t t t t)
                   (scalar "SELECT $1::varbit" '(("101111" . "bit")))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int2")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int2")))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int4")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int4")))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int8")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int8")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float4")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 85.0" '((-43.0 . "float4")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float8")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 85" '((-43.0 . "float8")))))
    (should (eql ?Q (scalar "SELECT $1" '((?Q . "char")))))
    (should (equal (list t nil) (row "SELECT $1, $2" '((t . "bool") (nil . "bool")))))
    (should (eql nil (scalar "SELECT $1 WHERE 0=1" '((42 . "int4")))))
    (should (string= "foobles" (scalar "SELECT $1" '(("foobles" . "text")))))
    (should (string= "foobles/var" (scalar "SELECT $1" '(("foobles/var" . "varchar")))))
    (should (string= "fooblé" (scalar "SELECT $1" '(("fooblé" . "text")))))
    (should (string= "Bîzzlô⚠️" (scalar "SELECT $1" '(("Bîzzlô⚠️" . "varchar")))))
    (should (string= "foobles" (scalar "SELECT $1 || $2" '(("foo" . "text") ("bles" . "text")))))
    (unless (zerop (car (pg-result (pg-exec con "SELECT COUNT(*) FROM pg_collation WHERE collname='fr_FR'") :tuple 0)))
      (should (string= "12 foé£èüñ¡" (scalar "SELECT lower($1) COLLATE \"fr_FR\"" '(("12 FOÉ£ÈÜÑ¡" . "text"))))))
    (should (equal "00:00:12" (scalar "SELECT $1::interval" '(("PT12S" . "text")))))
    (should (equal -1 (scalar "SELECT $1::int" '(("-1" . "text")))))
    (should (equal -1 (scalar "SELECT $1::int" '((-1 . "int4")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float4" '(("Infinity" . "text")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float4" '(("Infinity" . "float4")))))
    (should (equal (byte-to-string 0)
                   (scalar "SELECT $1::bytea" '(("\\000" . "text")))))
    (should (equal (byte-to-string 0)
                   (scalar "SELECT $1" `((,(byte-to-string 0) . "bytea")))))
    (should (equal (decode-hex-string "DEADBEEF")
                   (scalar "SELECT $1::bytea" '(("\\xDEADBEEF" . "text")))))
    (should (equal (decode-hex-string "DEADBEEF")
                   (scalar "SELECT $1" `((,(decode-hex-string "DEADBEEF") . "bytea")))))
    (let ((json (scalar "SELECT $1::json" '(("[66.7,-42.0,8]" . "text")))))
      (should (approx= 66.7 (aref json 0)))
      (should (approx= -42.0 (aref json 1))))
    (let ((json (scalar "SELECT $1::jsonb" '(("[66.7,-42.0,8]" . "text")))))
      (should (approx= 66.7 (aref json 0)))
      (should (approx= -42.0 (aref json 1))))
    (let ((json (scalar "SELECT $1::jsonb" '(("[5,7]" . "text")))))
      (should (eql 5 (aref json 0))))
    (let* ((ht (make-hash-table :test #'equal))
           (_ (puthash "say" "foobles" ht))
           (_ (puthash "biz" 42 ht))
           (json (scalar "SELECT $1::json" `((,ht . "json")))))
      (should (equal "foobles" (gethash "say" json)))
      (should (equal 42 (gethash "biz" json))))
    (when (pg-hstore-setup con)
      (let ((hs (scalar "SELECT $1::hstore" '(("a=>1,b=>2" . "text")))))
        (should (string= "1" (gethash "a" hs)))
        (should (eql 2 (hash-table-count hs)))))
    (should-error (scalar "SELECT * FROM" '(("a" . "text"))))
    (pg-sync con)
    (should-error (scalar "SELECT $1::int4" '(("2147483649" . "int4"))))
    (pg-sync con)))


(cl-defun pg-test-prepared/multifetch (con &optional (rows 1000))
  (message "Running multiple fetch/suspended portal test")
  (let* ((res (pg-exec-prepared con "SELECT generate_series(1, $1)"
                                `((,rows . "int4"))
                                :max-rows 10))
         (portal (pgresult-portal res))
         (counter 0))
    ;; check the results from the initial pg-exec-prepared
    (dolist (tuple (pg-result res :tuples))
      (should (eql (cl-first tuple) (cl-incf counter))))
    ;; keep fetching and checking more rows until the portal is complete
    (while (pg-result res :incomplete)
      (setq res (pg-fetch con res :max-rows 7))
      (dolist (tuple (pg-result res :tuples))
        (should (eql (cl-first tuple) (cl-incf counter)))))
    (pg-close-portal con portal))
  (message "multiple fetch/suspend portal test complete"))


(defun pg-test-basic (con)
  (cl-flet ((row (sql) (pg-result (pg-exec con sql) :tuple 0)))
    (should (equal (list t nil) (row "SELECT true, false")))
    (should (equal (list 42) (row "SELECT 42")))
    (should (equal (list -1) (row "SELECT -1::integer")))
    (should (equal (list "hey" "Jude") (row "SELECT 'hey', 'Jude'")))
    (should (equal (list nil) (row "SELECT NULL")))
    (should (equal nil (row "")))
    (should (equal nil (row "-- comment")))
    (should (equal (list 1 nil "all") (row "SELECT 1,NULL,'all'")))
    ;; QuestDB doesn't clearly identify itself in its version string, and doesn't implement CHR()
    (unless (cl-search "Visual C++ build 1914" (pg-backend-version con))
      (should (string= "Z" (car (row "SELECT chr(90)")))))
    (should (equal (list 12) (row "select length('(╯°□°)╯︵ ┻━┻')")))
    (should (eql nil (row " SELECT 3 where 1=0")))
    (should (string= "foo\nbar" (car (row "SELECT $$foo
bar$$"))))
    (should (string= "foo\tbar" (car (row "SELECT 'foo\tbar'"))))
    (should (string= "abcdef" (car (row "SELECT 'abc' || 'def'"))))
    (should (string= "howdy" (car (row "SELECT 'howdy'::text"))))
    (should (string= "gday" (car (row "SELECT 'gday'::varchar(20)"))))
    (should (string= (md5 "foobles") (car (row "SELECT md5('foobles')"))))
    ;; This setting defined in PostgreSQL v8.2. A value of 120007 means major version 12, minor
    ;; version 7. The value in pgcon-server-version-major is obtained by parsing the server_version
    ;; string sent by the backend on startup.
    (let* ((version-str (car (row "SELECT current_setting('server_version_num')")))
           (version-num (cl-parse-integer version-str)))
      (should (eql (pgcon-server-version-major con)
                   (/ version-num 10000))))))

(defun pg-test-insert (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (let ((count 100))
      (when (member "count_test" (pg-tables con))
        (pg-exec con "DROP TABLE count_test"))
      (pg-exec con "CREATE TABLE count_test(key int, val int)")
      (should (member "count_test" (pg-tables con)))
      (should (member "val" (pg-columns con "count_test")))
      (let ((user (or (getenv "PGEL_USER") "pgeltestuser")))
        (should (string= user (pg-table-owner con "count_test")))
        (should (string= user (pg-table-owner con (make-pg-qualified-name :name "count_test")))))
      (pg-exec con "COMMENT ON TABLE count_test IS 'Counting squared'")
      (pg-exec con "COMMENT ON COLUMN count_test.key IS 'preciouss'")
      (let* ((res (pg-exec con "SELECT obj_description('count_test'::regclass::oid, 'pg_class')"))
             (comment (cl-first (pg-result res :tuple 0))))
        (should (cl-search "squared" comment)))
      (should (cl-search "squared" (pg-table-comment con "count_test")))
      (should (cl-search "squared" (pg-table-comment con (make-pg-qualified-name :name "count_test"))))
      (let ((qn (make-pg-qualified-name :schema "public" :name "count_test")))
        (should (cl-search "squared" (pg-table-comment con qn))))
      (setf (pg-table-comment con "count_test") "Counting cubed")
      (should (cl-search "cubed" (pg-table-comment con "count_test")))
      (cl-loop for i from 1 to count
               for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                                 i (* i i))
               do (pg-exec con sql))
      (should (eql count (scalar "SELECT count(*) FROM count_test")))
      (should (eql (/ (* count (1+ count)) 2) (scalar "SELECT sum(key) FROM count_test")))
      (pg-exec con "DROP TABLE count_test")
      (should (not (member "count_test" (pg-tables con)))))))

(defun pg-test-insert/prepared (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (let ((count 100))
      (when (member "count_test" (pg-tables con))
        (pg-exec con "DROP TABLE count_test"))
      (pg-exec con "CREATE TABLE count_test(key int, val int)")
      (should (member "count_test" (pg-tables con)))
      (should (member "val" (pg-columns con "count_test")))
      (dotimes (i count)
        (pg-exec-prepared con "INSERT INTO count_test VALUES($1, $2)"
                          `((,i . "int4") (,(* i i) . "int4"))))
      (should (eql count (scalar "SELECT count(*) FROM count_test")))
      (should (eql (/ (* (1- count) count) 2) (scalar "SELECT sum(key) FROM count_test")))
      (pg-exec con "DROP TABLE count_test")
      (should (not (member "count_test" (pg-tables con)))))))

;; Testing for the date/time handling routines.
(defun pg-test-date (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-exec con "DROP TABLE IF EXISTS date_test")
    (pg-exec con "CREATE TABLE date_test(a timestamp, b time, c date)")
    (pg-exec con "INSERT INTO date_test VALUES "
             "(current_timestamp, 'now', current_date)")
    (let* ((res (pg-exec con "SELECT * FROM date_test"))
           (row (pg-result res :tuple 0)))
      (message "timestamp = %s" (cl-first row))
      (message "time = %s" (cl-second row))
      (message "date = %s" (cl-third row)))
    (pg-exec-prepared con "INSERT INTO date_test VALUES($1, $2, $3)"
                      `((,(pg-isodate-parser "2024-04-27T11:34:42" nil) . "timestamp")
                        ("11:34" . "time")
                        (,(pg-date-parser "2024-04-27" nil) . "date")))
    (should (eql 2 (scalar "SELECT COUNT(*) FROM date_test")))
    (pg-exec con "DROP TABLE date_test")
    ;; this fails on CockroachDB
    (unless (cl-search "CockroachDB" (pg-backend-version con))
      (should (equal (scalar "SELECT 'allballs'::time") "00:00:00")))
    (should (equal (scalar "SELECT '2022-10-01'::date") (encode-time 0 0 0 1 10 2022)))
    ;; When casting to DATE, the time portion is truncated
    (should (equal (scalar "SELECT '2063-03-31T22:13:02'::date")
                   (encode-time 0 0 0 31 3 2063)))
    ;; Here the hh:mm:ss are taken into account. The last 't' is for UTC timezone.
    (should (equal (scalar "SELECT '2063-03-31T22:13:02'::timestamp")
                   (encode-time 2 13 22 31 3 2063 nil t t)))
    (should (equal (scalar "SELECT 'PT42S'::interval") "00:00:42"))
    (should (equal (scalar "SELECT 'PT3H4M42S'::interval") "03:04:42"))
    (should (equal (scalar "select '05:00'::time") "05:00:00"))
    (should (equal (scalar "SELECT '2001-02-03 04:05:06'::timestamp")
                   (encode-time 6 5 4 3 2 2001 nil t)))))

(defun pg-test-numeric (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (should (eql -1 (scalar "SELECT '-1'::int")))
    (should (eql 128 (scalar "SELECT 128::int2")))
    (should (eql -5 (scalar "SELECT -5::int2")))
    (should (eql 27890 (scalar "SELECT 27890::int4")))
    (should (eql -128 (scalar "SELECT -128::int4")))
    (should (eql 66 (scalar "SELECT 66::int8")))
    (should (eql -1 (scalar "SELECT -1::int8")))
    (should (eql 42 (scalar "SELECT '42'::smallint")))
    (should (equal (make-bool-vector 1 nil) (scalar "SELECT 0::bit")))
    (should (equal (make-bool-vector 1 t) (scalar "SELECT 1::bit")))
    (should (equal (make-bool-vector 8 t) (scalar "SELECT CAST(255 as bit(8))")))
    ;; Emacs version prior to 27 can't coerce to bool-vector type
    (when (> emacs-major-version 26)
      (should (equal (cl-coerce (vector t nil t nil) 'bool-vector)
                     (scalar "SELECT '1010'::bit(4)")))
      (should (equal (cl-coerce (vector t nil nil t nil nil nil) 'bool-vector)
                     (scalar "SELECT b'1001000'")))
      (should (equal (cl-coerce (vector t nil t t t t) 'bool-vector)
                     (scalar "SELECT '101111'::varbit(6)"))))
    ;; (should (eql 66 (scalar "SELECT 66::money")))
    (should (eql (scalar "SELECT floor(42.3)") 42))
    (should (eql (scalar "SELECT trunc(43.3)") 43))
    (should (eql (scalar "SELECT trunc(-42.3)") -42))
    (unless (cl-search "CockroachDB" (pg-backend-version con))
      (should (eql (scalar "SELECT log(100)") 2))
      ;; bignums only supported from Emacs 27.2 onwards
      (when (fboundp 'bignump)
        (should (eql (scalar "SELECT factorial(25)") 15511210043330985984000000))))
    (should (approx= (scalar "SELECT pi()") 3.1415626))
    (should (approx= (scalar "SELECT -5.0") -5.0))
    (should (approx= (scalar "SELECT 5e-30") 5e-30))
    (should (approx= (scalar "SELECT 55.678::float4") 55.678))
    (should (approx= (scalar "SELECT 55.678::float8") 55.678))
    (should (approx= (scalar "SELECT 55.678::real") 55.678))
    (should (approx= (scalar "SELECT 55.678::numeric") 55.678))
    (should (approx= (scalar "SELECT -1000000000.123456789") -1000000000.123456789))
    (should (eql 1.0e+INF (scalar "SELECT 'Infinity'::float4")))
    (should (eql -1.0e+INF (scalar "SELECT '-Infinity'::float4")))
    (should (eql 1.0e+INF (scalar "SELECT 'Infinity'::float8")))
    (should (eql -1.0e+INF (scalar "SELECT '-Infinity'::float8")))
    (should (eql 0.0e+NaN (scalar "SELECT 'NaN'::float4")))
    (should (eql 0.0e+NaN (scalar "SELECT 'NaN'::float8")))
    (should (equal (list :range ?\[ 10 ?\) 20) (scalar "SELECT int4range(10, 20)")))
    (should (equal (list :range ?\[ -4 ?\) 6) (scalar "SELECT int8range(-4, 6)")))
    (should (equal (list :range ?\[ 5 ?\) 20) (scalar "SELECT int4range(5,15) + int4range(10,20)")))
    (should (equal (list :range ?\[ 10 ?\) 15) (scalar "SELECT int8range(5,15) * int8range(10,20)")))
    ;; Note that PostgreSQL has normalized the (3,7) discrete interval to [4,7)
    (should (equal (list :range ?\[ 4 ?\) 7) (scalar "SELECT '(3,7)'::int4range")))
    (should (equal (list :range ?\[ 4 ?\) 5) (scalar "SELECT '[4,4]'::int4range")))
    (should (equal (list :range ?\[ 2 ?\) 15) (scalar "SELECT int8range(1, 14, '(]')")))
    ;; this is the empty range
    (should (equal (list :range) (scalar "SELECT '[4,4)'::int4range")))
    (let ((range (scalar "SELECT numrange(33.33, 66.66)")))
      (should (eql :range (nth 0 range)))
      (should (eql ?\[ (nth 1 range)))
      (should (approx= 33.33 (nth 2 range)))
      (should (eql ?\) (nth 3 range)))
      (should (approx= 66.66 (nth 4 range))))
    (should (approx= -40.0 (scalar "SELECT upper(numrange(-50.0,-40.00))")))
    ;; range is unbounded on lower side
    (let ((range (scalar "SELECT numrange(NULL, 2.2)")))
      (should (eql :range (nth 0 range)))
      (should (eql ?\( (nth 1 range)))
      (should (eql nil (nth 2 range)))
      (should (eql ?\) (nth 3 range)))
      (should (approx= 2.2 (nth 4 range))))
    (should (equal (list :range ?\[ 42 ?\) nil) (scalar "SELECT int8range(42,NULL)")))
    (should (equal (list :range ?\( nil ?\) nil) (scalar "SELECT numrange(NULL, NULL)")))
    (should (string= (scalar "SELECT 42::decimal::text") "42"))
    (should (eql (scalar "SELECT char_length('foo')") 3))
    (should (string= (scalar "SELECT lower('FOO')") "foo"))
    (should (eql (scalar "SELECT ascii('a')") 97))
    (should (eql (length (scalar "SELECT repeat('Q', 5000)")) 5000))
    (should (string= (scalar "SELECT interval '1 day' + interval '3 days'") "4 days"))
    (should (eql (scalar "SELECT date '2001-10-01' - date '2001-09-28'") 3))))


;; https://www.postgresql.org/docs/current/datatype-xml.html#DATATYPE-XML-CREATING
;;
;; We are handling XML as an Emacs Lisp string. PostgreSQL is not always compiled with
;; XML support, so check for that first.
(defun pg-test-xml (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (unless (or (cl-search "CockroachDB" (pg-backend-version con))
                (cl-search "-YB-" (pg-backend-version con)))
      (unless (zerop (scalar "SELECT COUNT(*) FROM pg_type WHERE typname='xml'"))
        (should (string= "<foo attr=\"45\">bar</foo>"
                         (scalar "SELECT XMLPARSE (CONTENT '<foo attr=\"45\">bar</foo>')")))
        (should (string= (scalar "SELECT xmlforest('abc' AS foo, 123 AS bar)")
                         "<foo>abc</foo><bar>123</bar>"))))))

;; https://www.postgresql.org/docs/current/datatype-uuid.html
(defun pg-test-uuid (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (string= "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                     (scalar "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid")))
    (should (string= "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                     (scalar "SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid")))
    (dotimes (_i 30)
      (let ((uuid (scalar "SELECT gen_random_uuid()"))
            (re (concat "\\<[[:xdigit:]]\\{8\\}-"
                        "[[:xdigit:]]\\{4\\}-"
                        "[[:xdigit:]]\\{4\\}-"
                        "[[:xdigit:]]\\{4\\}-"
                        "[[:xdigit:]]\\{12\\}\\>")))
        (should (string-match re uuid))))))


;; https://www.postgresql.org/docs/current/collation.html
;;
;; Case support in PostgreSQL (lower() and upper()) depend on the current collation rules. To remove
;; dependency on the collation specified when creating the current database, specify the desired
;; collation explicitly.
(defun pg-test-collation (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    ;; Check whether fr_FR collation is already available
    (unless (zerop (scalar "SELECT COUNT(*) FROM pg_collation WHERE collname='fr_FR'"))
      (should (string= (scalar "SELECT lower('FÔÖÉ' COLLATE \"fr_FR\")") "fôöé"))
      (should (string= (scalar "SELECT lower('FÔ🐘💥bz' COLLATE \"fr_FR\")") "fô🐘💥bz")))
    ;; Check whether PostgreSQL was compiled with ICU support. If so, create a collation with ICU
    ;; provider.
    (unless (zerop (scalar "SELECT COUNT(*) FROM pg_collation WHERE collname='und-x-icu'"))
      (scalar "CREATE COLLATION IF NOT EXISTS \"french\" (provider = icu, locale = 'fr_FR')")
      (should (string= (scalar "SELECT lower('FÔÖÉ' COLLATE \"french\")") "fôöé"))
      (should (string= (scalar "SELECT lower('FÔ🐘💥bz' COLLATE \"french\")") "fô🐘💥bz")))))


;; tests for BYTEA type (https://www.postgresql.org/docs/15/functions-binarystring.html)
(defun pg-test-bytea (con)
  (pg-exec con "DROP TABLE IF EXISTS byteatest")
  (pg-exec con "CREATE TABLE byteatest(blob BYTEA, tag int)")
  (pg-exec con "INSERT INTO byteatest VALUES('warning\\000'::bytea, 1)")
  (pg-exec con "INSERT INTO byteatest VALUES('\\001\\002\\003'::bytea, 2)")
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (equal (byte-to-string 0) (scalar "SELECT '\\000'::bytea")))
    (should (equal (byte-to-string ?') (scalar "SELECT ''''::bytea")))
    (should (equal (decode-hex-string "DEADBEEF") (scalar "SELECT '\\xDEADBEEF'::bytea")))
    (should (equal (string 1 3 5) (scalar "SELECT '\\001\\003\\005'::bytea")))
    (should (equal (decode-hex-string "123456789a00bcde")
                   (scalar "SELECT '\\x123456'::bytea || '\\x789a00bcde'::bytea")))
    (should (equal (secure-hash 'sha256 "foobles")
                   (encode-hex-string (scalar "SELECT sha256('foobles'::bytea)"))))
    (should (equal (base64-encode-string "foobles")
                   (scalar "SELECT encode('foobles', 'base64')")))
    (should (equal "foobles" (scalar "SELECT decode('Zm9vYmxlcw==', 'base64')")))
    (should (equal "warning " (scalar "SELECT blob FROM byteatest WHERE tag=1")))
    (should (equal (string 1 2 3) (scalar "SELECT blob FROM byteatest WHERE tag=2")))
    ;; When sending binary data to PostgreSQL, either encode all potentially problematic octets
    ;; like NUL (as above), or send base64-encoded content and decode in PostgreSQL.
    (let* ((size 512)
           (random-octets (make-string size 0)))
      (dotimes (i size)
        (setf (aref random-octets i) (random 256)))
      (setf (aref random-octets 0) 0)
      (pg-exec con (format "INSERT INTO byteatest VALUES (decode('%s', 'base64'), 3)"
                            (base64-encode-string random-octets)))
      (should (equal random-octets (scalar "SELECT blob FROM byteatest WHERE tag=3")))))
     (pg-exec con "DROP TABLE byteatest"))

(defun pg-test-sequence (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-exec con "DROP SEQUENCE IF EXISTS foo_seq")
    (pg-exec con "CREATE SEQUENCE IF NOT EXISTS foo_seq INCREMENT 20 START WITH 400")
    (should (equal 400 (scalar "SELECT nextval('foo_seq')")))
    (should (equal 400 (scalar "SELECT last_value FROM pg_sequences WHERE sequencename='foo_seq'")))
    (should (equal 420 (scalar "SELECT nextval('foo_seq')")))
    (should (equal 440 (scalar "SELECT nextval('foo_seq')")))
    (pg-exec con "DROP SEQUENCE foo_seq")))

(defun pg-test-array (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (should (equal (vector 7 8) (scalar "SELECT ARRAY[7,8]")))
    (should (equal (vector 9 10 11) (scalar "SELECT '{9,10,11}'::int[]")))
    (should (equal (vector 1234) (scalar "SELECT ARRAY[1234::int2]")))
    (should (equal (vector -3456) (scalar "SELECT ARRAY[-3456::int4]")))
    (should (equal (vector 9987) (scalar "SELECT ARRAY[9987::int8]")))
    (should (equal (vector 2 8) (scalar "SELECT ARRAY[2,8]")))
    (let ((vec (scalar "SELECT ARRAY[3.14::float]")))
      (should (approx= 3.14 (aref vec 0))))
    (should (equal (vector 4 20) (scalar "SELECT ARRAY[4] || 20")))
    (should (eql 6 (scalar "SELECT array_length('{1,2,3,4,5,6}'::int4[], 1)")))
    (should (equal (vector 42) (scalar "SELECT array_agg(42)")))
    (should (equal (vector 45 67 89) (scalar "SELECT '{45,67,89}'::smallint[]")))
    (should (equal (vector t nil t nil t)
                   (scalar "SELECT '{true, false, true, false, true}'::bool[]")))
    (should (equal (vector ?A ?z ?5) (scalar "SELECT '{A,z,5}'::char[]")))
    ;; this is returning _bpchar
    (should (equal (vector ?a ?b ?c) (scalar "SELECT CAST('{a,b,c}' AS CHAR[])")))
    (should (equal (vector "foo" "bar") (scalar "SELECT '{foo, bar}'::text[]")))
    (let ((vec (scalar "SELECT ARRAY[44.3, 8999.5]")))
      (should (equal 2 (length vec)))
      (should (approx= 44.3 (aref vec 0)))
      (should (approx= 8999.5 (aref vec 1))))))

;; TODO: we do not currently handle multidimension arrays correctly
;; (should (equal (vector (vector 4 5) (vector 6 7))
;;                (scalar "SELECT '{{4,5},{6,7}}'::int8[][]")))))

;; Schemas for qualified names such as public.tablename. 
(defun pg-test-schemas (con)
  (let ((res (pg-exec con "CREATE SCHEMA IF NOT EXISTS custom")))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((res (pg-exec con "CREATE TABLE IF NOT EXISTS custom.newtable(id INT4)")))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((tables (pg-tables con)))
    (should (cl-find "newtable" tables
                     :test #'string=
                     :key (lambda (tbl) (if (pg-qualified-name-p tbl)
                                            (pg-qualified-name-name tbl)
                                          tbl)))))
  ;; now try some strange names for schemas and tables to test quoting
  (let* ((sql (format "CREATE SCHEMA IF NOT EXISTS %s" (pg-escape-identifier "fan.cy")))
         (res (pg-exec con sql)))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let* ((sql (format "CREATE TABLE IF NOT EXISTS %s.%s(id INT4)"
                      (pg-escape-identifier "fan.cy")
                      (pg-escape-identifier "re'ally")))
         (res (pg-exec con sql)))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((tables (pg-tables con)))
    (should (cl-find "re'ally" tables
                     :test #'string=
                     :key (lambda (tbl) (if (pg-qualified-name-p tbl)
                                            (pg-qualified-name-name tbl)
                                          tbl)))))
  (let* ((sql (format "CREATE TABLE IF NOT EXISTS %s.%s(id INT4)"
                      (pg-escape-identifier "fan.cy")
                      (pg-escape-identifier "en-ough")))
         (res (pg-exec con sql)))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((tables (pg-tables con)))
    (should (cl-find "en-ough" tables
                     :test #'string=
                     :key (lambda (tbl) (if (pg-qualified-name-p tbl)
                                            (pg-qualified-name-name tbl)
                                          tbl)))))
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "tri\"cks"))
         (sql (format "CREATE TABLE IF NOT EXISTS %s(id INT4)"
                      (pg-print-qualified-name qn)))
         (res (pg-exec con sql)))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((tables (pg-tables con)))
    (should (cl-find "tri\"cks" tables
                     :test #'string=
                     :key (lambda (tbl) (if (pg-qualified-name-p tbl)
                                            (pg-qualified-name-name tbl)
                                          tbl)))))
  ;; SQL query using "manual" escaping of the components of a qualified name
  (let* ((schema (pg-escape-identifier "fan.cy"))
         (table (pg-escape-identifier "re'ally"))
         (sql (format "INSERT INTO %s.%s VALUES($1)" schema table))
         (res (pg-exec-prepared con sql `((42 . "int4")))))
    (should (zerop (cl-search "INSERT" (pg-result res :status)))))
  ;; Dynamic SQL query using our printing support for qualified names
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "re'ally"))
         (pqn (pg-print-qualified-name qn))
         (sql (format "INSERT INTO %s VALUES($1)" pqn))
         (res (pg-exec-prepared con sql `((44 . "int4")))))
    (should (zerop (cl-search "INSERT" (pg-result res :status)))))
  ;; SQL function call using a parameter and our printing support for qualified names
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "re'ally"))
         (pqn (pg-print-qualified-name qn))
         (sql "SELECT pg_total_relation_size($1)")
         (res (pg-exec-prepared con sql `((,pqn . "text"))))
         (size (cl-first (pg-result res :tuple 0))))
    (should (<= 0 size 10000)))
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "tri\"cks"))
         (pqn (pg-print-qualified-name qn))
         (sql "SELECT pg_total_relation_size($1)")
         (res (pg-exec-prepared con sql `((,pqn . "text"))))
         (size (cl-first (pg-result res :tuple 0))))
    (should (<= 0 size 10000)))
  (let ((res (pg-exec con "DROP TABLE custom.newtable")))
    (should (zerop (cl-search "DROP" (pg-result res :status)))))
  (let ((res (pg-exec con (format "DROP TABLE %s.%s"
                                  (pg-escape-identifier "fan.cy")
                                  (pg-escape-identifier "re'ally")))))
    (should (zerop (cl-search "DROP" (pg-result res :status)))))
  (let ((res (pg-exec con (format "DROP TABLE %s.%s"
                                  (pg-escape-identifier "fan.cy")
                                  (pg-escape-identifier "en-ough")))))
    (should (zerop (cl-search "DROP" (pg-result res :status)))))
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "tri\"cks"))
         (pqn (pg-print-qualified-name qn))
         (res (pg-exec con (format "DROP TABLE %s" pqn))))
    (should (zerop (cl-search "DROP" (pg-result res :status)))))
  (let ((res (pg-exec con "DROP SCHEMA custom")))
    (should (zerop (cl-search "DROP" (pg-result res :status)))))
  (let ((res (pg-exec con (format "DROP SCHEMA %s" (pg-escape-identifier "fan.cy")))))
    (should (zerop (cl-search "DROP" (pg-result res :status))))))


;; https://www.postgresql.org/docs/15/functions-json.html
(defun pg-test-json (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (should (eql 42 (scalar "SELECT to_json(42)")))
    (let ((json (scalar "SELECT '[5,7]'::json")))
      (should (eql 5 (aref json 0))))
    (let ((json (scalar "SELECT '[5,7]'::jsonb")))
      (should (eql 5 (aref json 0))))
    (let ((json (scalar "SELECT '[66.7,-42.0,8]'::json")))
      (should (approx= 66.7 (aref json 0)))
      (should (approx= -42.0 (aref json 1))))
    (let ((json (scalar "SELECT '[66.7,-42.0,8]'::jsonb")))
      (should (approx= 66.7 (aref json 0)))
      (should (approx= -42.0 (aref json 1))))
    ;; JSON null in JSONB type is not the same as PostgreSQL NULL value!
    (should (eql nil (scalar "SELECT 'null'::jsonb is null")))
    (should (eql nil (scalar "SELECT '{\"name\": null}'::jsonb->'name' IS NULL")))
    ;; JSON handling (handling of dictionaries, of NULL, false, [] and {}, etc.) differs between
    ;; the native JSON support and the json elisp libary. We only test the native support.
    (when (and (fboundp 'json-parse-string)
               (fboundp 'json-available-p)
               (json-available-p))
      (should (eql :null (scalar "SELECT 'null'::json")))
      (should (equal (vector) (scalar "SELECT '[]'::json")))
      (should (equal (vector) (scalar "SELECT '[]'::jsonb")))
      (let ((json (scalar "SELECT '{}'::json")))
        (should (eql 0 (hash-table-count json))))
      (let ((json (scalar "SELECT '{}'::jsonb")))
        (should (eql 0 (hash-table-count json))))
      (should (equal (vector :null) (scalar "SELECT '[null]'::json")))
      (should (equal (vector :null) (scalar "SELECT '[null]'::jsonb")))
      (should (equal (vector 42 :null 77) (scalar "SELECT '[42,null,77]'::json")))
      (should (equal :null (gethash "a" (scalar "SELECT '{\"a\": null}'::json"))))
      (should (equal (vector t :false 42) (scalar "SELECT '[true,false,42]'::json")))
      (should (equal (vector t :false 42) (scalar "SELECT '[true,false,42]'::jsonb")))
      (let ((json (scalar "SELECT json_object_agg(42, 66)")))
        (should (eql 66 (gethash "42" json))))
      (let ((json (scalar "SELECT '{\"a\":1,\"b\":-22}'::json")))
        (should (eql 1 (gethash "a" json)))
        (should (eql -22 (gethash "b" json))))
      (let ((json (scalar "SELECT '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json")))
        (should (string= "bar" (gethash "b" (aref json 1)))))
      (let ((json (scalar "SELECT '{\"a\": [0,1,2,null]}'::json")))
        (should (eql 2 (aref (gethash "a" json) 2)))))))


(defun pg-test-server-prepare (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-exec con "PREPARE foobles AS SELECT 2 * $1::int4")
    (should (eql 66 (scalar "EXECUTE foobles(33)")))))


(defun pg-test-hstore (con)
  ;; We need to call this before using HSTORE datatypes to load the extension if necessary, and
  ;; to set up our parser support for the HSTORE type.
  (when (pg-hstore-setup con)
    (message "Testing HSTORE extension")
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
      (let ((hs (scalar "SELECT 'foo=>bar'::hstore")))
        (should (string= "bar" (gethash "foo" hs)))
        (should (eql 1 (hash-table-count hs))))
      (let ((hs (scalar "SELECT 'a=>1,b=>2'::hstore")))
        (should (string= "1" (gethash "a" hs)))
        (should (eql 2 (hash-table-count hs))))
      ;; There is no guarantee as to the value stored for the 'a' key (duplicate)
      (let ((hs (scalar "SELECT 'a=>1,foobles=>2,a=>66'::hstore")))
        (should (eql 2 (hash-table-count hs)))
        (should (string= "2" (gethash "foobles" hs))))
      (let ((hs (scalar "SELECT 'a=>b, c=>d'::hstore || 'c=>x, d=>q'::hstore")))
        (should (string= "x" (gethash "c" hs))))
      (let ((hs (scalar "SELECT 'a=>1, b=>2, c=>3'::hstore - 'b'::text")))
        (should (eql 2 (hash-table-count hs)))
        (should (string= "3" (gethash "c" hs))))
      (let ((hs (scalar "SELECT hstore(ARRAY['a','1','b','42'])")))
        (should (eql 2 (hash-table-count hs)))
        (should (string= "42" (gethash "b" hs))))
      (let ((hs (scalar "SELECT hstore('aaa=>bq, b=>NULL, \"\"=>1')")))
        (should (eql nil (gethash "b" hs)))
        (should (string= "1" (gethash "" hs))))
      (let ((arr (scalar "SELECT akeys('biz=>NULL,baz=>42,boz=>66'::hstore)")))
        (should (cl-find "biz" arr :test #'string=))
        (should (cl-find "boz" arr :test #'string=)))
      ;; see https://github.com/postgres/postgres/blob/9c40db3b02a41e978ebeb2c61930498a36812bbf/contrib/hstore/sql/hstore_utf8.sql
      (let ((hs (scalar "SELECT 'ą=>é'::hstore")))
        (should (string= (gethash "ą" hs) "é")))
      ;; now test serialization support
      (pg-exec con "DROP TABLE IF EXISTS hstored")
      (pg-exec con "CREATE TABLE hstored(id SERIAL PRIMARY KEY, meta HSTORE)")
      (dotimes (i 10)
        (let ((hs (make-hash-table :test #'equal)))
          (puthash (format "foobles-%d" i) (format "bazzles-%d" i) hs)
          (puthash (format "a%d" (1+ i)) (format "%d" (- i)) hs)
          (pg-exec-prepared con "INSERT INTO hstored(meta) VALUES ($1)"
                            `((,hs . "hstore")))))
      (let ((rows (scalar "SELECT count(*) FROM hstored")))
        (should (eql 10 rows)))
      (let* ((res (pg-exec con "SELECT meta FROM hstored"))
             (rows (pg-result res :tuples)))
        (dolist (ht (mapcar #'car rows))
          (maphash (lambda (k v)
                     (should (or (cl-search "foobles" k)
                                 (eql ?a (aref k 0))))
                     (should (or (cl-search "bazzles" v)
                                 (ignore-errors (string-to-number v)))))
                   ht))))))


;; Testing support for the pgvector extension.
(defun pg-test-vector (con)
  (when (pg-vector-setup con)
    (message "Testing pgvector extension")
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
      (let ((v (scalar "SELECT '[4,5,6]'::vector")))
        (should (eql 4 (aref v 0))))
      (pg-exec con "DROP TABLE IF EXISTS items")
      ;; "BIGINT GENERATED ALWAYS AS IDENTITY" is more standard than "BIGSERIAL"
      (pg-exec con "CREATE TABLE items (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, embedding vector(4))")
      (dotimes (_ 1000)
        (let ((new (vector (random 55) (random 66) (random 77) (random 88))))
          (pg-exec-prepared con "INSERT INTO items(embedding) VALUES($1)"
                            `((,new . "vector")))))
      (let ((res (pg-exec con "SELECT embedding FROM items ORDER BY embedding <-> '[1,1,1,1]' LIMIT 1")))
        (message "PGVECTOR> closest = %s" (car (pg-result res :tuple 0))))
      (pg-exec con "DROP TABLE items"))))

;; Testing support for the tsvector type used for full text search.
;; See https://www.postgresql.org/docs/current/datatype-textsearch.html
(defun pg-test-tsvector (con)
  (message "Testing tsvector type support")
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (equal (list (make-pg-ts :lexeme "foo")) (scalar "SELECT 'foo'::tsvector")))
    (let ((tsvec (scalar "SELECT 'foo bar'::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "foo") tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "bar") tsvec :test #'equal)))
    (let ((tsvec (scalar "SELECT $$the lexeme '    ' contains spaces$$::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "contains") tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "    ") tsvec :test #'equal)))
    (let ((tsvec (scalar "SELECT 'a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12'::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "and" :weighted-positions '((8 . :D))) tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "mat" :weighted-positions '((7 . :D))) tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "fat" :weighted-positions '((2 . :D) (11 . :D))) tsvec :test #'equal)))
    (let ((tsvec (scalar "SELECT 'a:1A fat:2B,4C cat:5D'::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "a" :weighted-positions '((1 . :A))) tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "fat" :weighted-positions '((2 . :B) (4 . :C))) tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "cat" :weighted-positions '((5 . :D))) tsvec :test #'equal)))
    (let ((tsvec (scalar "SELECT $$the lexeme 'Joe''s' contains a quote$$::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "a") tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "quote") tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "Joe's") tsvec :test #'equal)))
    (let ((tsvec (scalar "SELECT 'The Fat Rats'::tsvector")))
      (should (cl-find (make-pg-ts :lexeme "Fat") tsvec :test #'equal))
      (should (cl-find (make-pg-ts :lexeme "Rats") tsvec :test #'equal)))))


;; https://www.postgresql.org/docs/current/sql-copy.html
(defun pg-test-copy (con)
  (cl-flet ((ascii (n) (+ ?A (mod n 26)))
            (random-word () (apply #'string (cl-loop for count to 10 collect (+ ?a (random 26))))))
    (pg-exec con "DROP TABLE IF EXISTS copy_tsv")
    (pg-exec con "CREATE TABLE copy_tsv (a INTEGER, b CHAR, c TEXT)")
    (with-temp-buffer
      (dotimes (i 42)
        (insert (format "%d\t%c\t%s\n" i (ascii i) (random-word))))
      (pg-copy-from-buffer con "COPY copy_tsv(a,b,c) FROM STDIN" (current-buffer))
      (let ((res (pg-exec con "SELECT count(*) FROM copy_tsv")))
        (should (eql 42 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec con "SELECT sum(a) FROM copy_tsv")))
        (should (eql 861 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec con "SELECT * FROM copy_tsv LIMIT 5")))
        (message "COPYTSV> %s" (pg-result res :tuples))))
    (pg-exec con "DROP TABLE copy_tsv")
    (pg-exec con "DROP TABLE IF EXISTS copy_csv")
    (pg-exec con "CREATE TABLE copy_csv (a INT2, b INTEGER, c CHAR, d TEXT)")
    (with-temp-buffer
      (dotimes (i 500)
        (insert (format "%d,%d,%c,%s\n" i (* i i) (ascii i) (random-word))))
      (dotimes (i 500)
        ;; Check that quoted strings are accepted by PostgreSQL
        (insert (format "%d,%d,%c,\"%sfôt\"\n" i (* i i) (ascii i) (random-word))))
      (pg-copy-from-buffer con "COPY copy_csv(a,b,c,d) FROM STDIN WITH (FORMAT CSV)" (current-buffer))
      (let ((res (pg-exec con "SELECT count(*) FROM copy_csv")))
        (should (eql 1000 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec con "SELECT max(b) FROM copy_csv")))
        (should (eql (* 499 499) (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec con "SELECT * FROM copy_csv LIMIT 3")))
        (message "COPYCSV> %s" (pg-result res :tuples)))
      (pg-exec con "DROP TABLE copy_csv"))
    ;; testing COPY TO STDOUT
    (pg-exec con "DROP TABLE IF EXISTS copy_from")
    (pg-exec con "CREATE TABLE copy_from (a INT2, b INTEGER, c CHAR, d TEXT)")
    (dotimes (_i 100)
      (pg-exec-prepared con "INSERT INTO copy_from VALUES($1,$2,$3,$4)"
                        `((,(random 100) . "int2")
                          (,(- (random 1000000) 500000) . "int4")
                          (,(+ ?A (random 26)) . "char")
                          (,(random-word) . "text"))))
    (with-temp-buffer
      (pg-copy-to-buffer con "COPY copy_from TO STDOUT" (current-buffer))
      ;; We should have 100 lines in the output buffer
      (should (eql 100 (cl-first (buffer-line-statistics))))
      (should (eql 300 (cl-count ?\t (buffer-string)))))
    (with-temp-buffer
      (pg-copy-to-buffer con "COPY copy_from TO STDOUT WITH (FORMAT CSV, HEADER TRUE)" (current-buffer))
      (should (eql 101 (cl-first (buffer-line-statistics))))
      (should (eql 303 (cl-count ?, (buffer-string)))))
    (pg-exec con "DROP TABLE copy_from")))

;; Test COPY FROM STDIN on a non-trivial CSV file, which contains UTF-8 data
(defun pg-test-copy-large (con)
  (with-temp-buffer
    (url-insert-file-contents "https://www.data.gouv.fr/fr/datasets/r/51606633-fb13-4820-b795-9a2a575a72f1")
    (pg-exec con "DROP TABLE IF EXISTS cities")
    (pg-exec con "CREATE TABLE cities(
              insee_code TEXT NOT NULL,
              city_code TEXT,
              zip_code NUMERIC,
              label TEXT NOT NULL,
              latitude FLOAT,
              longitude FLOAT,
              department_name TEXT,
              department_number VARCHAR(3),
              region_name TEXT,
              region_geojson_name TEXT)")
    (pg-copy-from-buffer con "COPY cities FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE)" (current-buffer))
    (pg-exec con "ALTER TABLE cities DROP COLUMN region_name")
    (pg-exec con "ALTER TABLE cities DROP COLUMN region_geojson_name")
    (pg-exec con "ALTER TABLE cities DROP COLUMN label")
    (pg-exec con "DROP TABLE cities")))



;; "SELECT xmlcomment("42") -> "<!--42-->"
(defun pg-test-xmlbinary (_con)
  nil)

;; Testing for the data access functions. Expected output is something
;; like
;;
;; ==============================================
;; status of CREATE is CREATE
;; status of INSERT is INSERT 22506 1
;; oid of INSERT is 22506
;; status of SELECT is SELECT
;; attributes of SELECT are ((a 23 4) (b 1043 65535))
;; tuples of SELECT are ((3 zae) (66 poiu))
;; second tuple of SELECT is (66 poiu)
;; status of DROP is DROP
;; ==============================================
(defun pg-test-result (con)
  (pg-exec con "DROP TABLE IF EXISTS resulttest")
  (let ((r1 (pg-exec con "CREATE TABLE resulttest (a int, b VARCHAR(4))"))
        (r2 (pg-exec con "INSERT INTO resulttest VALUES (3, 'zae')"))
        (r3 (pg-exec con "INSERT INTO resulttest VALUES (66, 'poiu')"))
        (r4 (pg-exec con "SELECT * FROM resulttest"))
        (r5 (pg-exec con "UPDATE resulttest SET b='foob' WHERE a=66"))
        (r6 (pg-exec con "SELECT b FROM resulttest WHERE a=66"))
        (r7 (pg-exec con "DROP TABLE resulttest"))
        (r8 (pg-exec con "SELECT generate_series(1, 10)")))
    (message "==============================================")
    (message "status of CREATE is %s" (pg-result r1 :status))
    (should (string= "CREATE TABLE" (pg-result r1 :status)))
    (message "status of INSERT is %s" (pg-result r2 :status))
    (should (string= "INSERT 0 1" (pg-result r2 :status)))
    (message "oid of INSERT is %s"    (pg-result r2 :oid))
    (should (integerp (pg-result r2 :oid)))
    (should (string= "INSERT 0 1" (pg-result r3 :status)))
    (message "status of SELECT is %s" (pg-result r4 :status))
    (should (string= "SELECT 2" (pg-result r4 :status)))
    (message "attributes of SELECT are %s" (pg-result r4 :attributes))
    (message "tuples of SELECT are %s" (pg-result r4 :tuples))
    (should (eql 2 (length (pg-result r4 :tuples))))
    (message "second tuple of SELECT is %s" (pg-result r4 :tuple 1))
    (should (string= "UPDATE 1" (pg-result r5 :status)))
    (should (string= "foob" (car (pg-result r6 :tuple 0))))
    (message "status of DROP is %s" (pg-result r7 :status))
    (should (string= "DROP TABLE" (pg-result r7 :status)))
    (should (eql (length (pg-result r8 :tuples)) 10))
    (message "=============================================="))
   (let ((res (pg-exec con "SELECT 1 UNION SELECT 2")))
     (should (equal '((1) (2)) (pg-result res :tuples))))
   (let ((res (pg-exec con "SELECT 1,2,3,'soleil'")))
     (should (equal '(1 2 3 "soleil") (pg-result res :tuple 0))))
   (let ((res (pg-exec con "SELECT 42 as z")))
     (should (string= "z" (caar (pg-result res :attributes)))))
   (let* ((res (pg-exec con "SELECT 42 as z, 'bob' as bob"))
          (attr (pg-result res :attributes)))
     (should (string= "z" (caar attr)))
     (should (string= "bob" (caadr attr))))
   (pg-exec con "DROP TYPE IF EXISTS FRUIT")
   (pg-exec con "CREATE TYPE FRUIT AS ENUM('banana', 'orange', 'apple', 'pear')")
   (let* ((res (pg-exec con "SELECT 'apple'::fruit"))
          (attr (pg-result res :attributes)))
     (should (string= "apple" (car (pg-result res :tuple 0))))
     (should (string= "fruit" (caar attr))))
   (let* ((res (pg-exec con "SELECT 32 as éléphant"))
          (attr (pg-result res :attributes)))
     (should (string= "éléphant" (caar attr)))
     (should (eql 32 (car (pg-result res :tuple 0)))))
   ;; Test PREPARE / EXECUTE
   (pg-exec con "PREPARE ps42 AS SELECT 42")
   (let ((res (pg-exec con "EXECUTE ps42")))
     (should (eql 42 (car (pg-result res :tuple 0)))))
   (pg-exec con "DEALLOCATE ps42")
   ;; Test cursors
   (let ((res (pg-exec con "BEGIN")))
     (should (string= "BEGIN" (pg-result res :status))))
   (pg-exec con "CREATE TEMPORARY TABLE cursor_test (a INTEGER, b TEXT)")
   (dotimes (i 10)
     (pg-exec con (format "INSERT INTO cursor_test VALUES(%d, '%d')" i i)))
   (let ((res (pg-exec con "DECLARE crsr42 CURSOR FOR SELECT * FROM cursor_test WHERE a=2")))
     (should (string= "DECLARE CURSOR" (pg-result res :status))))
   (let ((res (pg-exec con "FETCH 1000 FROM crsr42")))
     (should (string= "FETCH 1" (pg-result res :status)))
     (should (eql 1 (length (pg-result res :tuples)))))
   (let ((res (pg-exec con "CLOSE crsr42")))
     (should (string= "CLOSE CURSOR" (pg-result res :status))))
   (let ((res (pg-exec con "COMMIT")))
     (should (string= "COMMIT" (pg-result res :status))))
   (let ((res (pg-exec con "EXPLAIN ANALYZE SELECT 42")))
     (should (string= "EXPLAIN" (pg-result res :status)))
     (should (cl-every (lambda (r) (stringp (car r))) (pg-result res :tuples))))
   ;; check query with empty column list
   (let ((res (pg-exec con "SELECT from information_schema.routines")))
     (should (eql nil (pg-result res :attributes)))
     (should (cl-every #'null (pg-result res :tuples)))))


(defun pg-test-createdb (con)
  (when (member "pgeltestextra" (pg-databases con))
    (pg-exec con "DROP DATABASE pgeltestextra"))
  (pg-exec con "CREATE DATABASE pgeltestextra")
  (should (member "pgeltestextra" (pg-databases con)))
  ;; CockroachDB and YugabyteDB don't implement REINDEX. Also, REINDEX at the database level is
  ;; disabled on certain installations (e.g. Supabase), so we check reindexing of a table.
  (unless (or (cl-search "CockroachDB" (pg-backend-version con))
              (cl-search "-YB-" (pg-backend-version con)))
    (pg-exec con "DROP TABLE IF EXISTS foobles")
    (pg-exec con "CREATE TABLE foobles(a INTEGER, b TEXT)")
    (pg-exec con "CREATE INDEX idx_foobles ON foobles(a)")
    (pg-exec con "INSERT INTO foobles VALUES (42, 'foo')")
    (pg-exec con "INSERT INTO foobles VALUES (66, 'bizzle')")
    (when (> (pgcon-server-version-major con) 11)
      (pg-exec con "REINDEX TABLE CONCURRENTLY foobles"))
    (pg-exec con "DROP TABLE foobles"))
  (let* ((r (pg-exec con "SHOW ALL"))
         (config (pg-result r :tuples)))
    (cl-loop for row in config
             when (string= "port" (car row))
             do (message "Connected to PostgreSQL on port %s" (cadr row))))
  (pg-exec con "DROP DATABASE pgeltestextra"))

(defun pg-test-unicode-names (con)
  (when (member "pgel😎" (pg-databases con))
    (pg-exec con "DROP DATABASE pgel😎"))
  (pg-exec con "CREATE DATABASE pgel😎")
  (should (member "pgel😎" (pg-databases con)))
  (pg-exec con "DROP DATABASE pgel😎")
  (pg-exec con "CREATE TEMPORARY TABLE pgel😏(data TEXT)")
  (pg-exec con "INSERT INTO pgel😏 VALUES('Foobles')")
  (let ((r (pg-exec con "SELECT * FROM pgel😏")))
    (should (eql 1 (length (pg-result r :tuples)))))
  (pg-exec con "CREATE TEMPORARY TABLE pgeltestunicode(pg→el TEXT)")
  (pg-exec con "INSERT INTO pgeltestunicode(pg→el) VALUES ('Foobles')")
  (pg-exec con "INSERT INTO pgeltestunicode(pg→el) VALUES ('Bizzles')")
  (let ((r (pg-exec con "SELECT pg→el FROM pgeltestunicode")))
    (should (eql 2 (length (pg-result r :tuples))))))

(defun pg-test-returning (con)
  (when (member "pgeltestr" (pg-tables con))
    (pg-exec con "DROP TABLE pgeltestr"))
  (pg-exec con "CREATE TABLE pgeltestr(id SERIAL, data TEXT)")
  (let* ((res (pg-exec con "INSERT INTO pgeltestr(data) VALUES ('Foobles') RETURNING id"))
         (id (pg-result res :tuple 0))
         (res (pg-exec con (format "SELECT data from pgeltestr WHERE id=%s" id))))
    (should (string= (car (pg-result res :tuple 0)) "Foobles")))
  (pg-exec con "DROP TABLE pgeltestr"))

;; Test our support for handling ParameterStatus messages, via the pg-parameter-change-functions
;; variable. When we change the session timezone, the backend should send us a ParameterStatus
;; message with TimeZone=<new-value>.
(defun pg-test-parameter-change-handlers (con)
  (message "Testing parameter-change-functions hook")
  (let ((handler-called nil))
    (cl-flet ((tz-handler (_con name _value)
                (when (string= "TimeZone" name)
                  (setq handler-called t))))
      (cl-pushnew #'tz-handler pg-parameter-change-functions)
      ;; The backend will only send us a ParameterStatus message when the timezone changes, so
      ;; we make two changes to make sure at least one of them generates a ParameterStatus message.
      (pg-exec con "SET SESSION TIME ZONE 'Europe/Paris'")
      (pg-exec con "SET SESSION TIME ZONE 'America/Chicago'")
      (pg-exec con "SELECT 42")
      (should (eql t handler-called)))))

;; Check that we raise errors when expected, that we resync with the backend after an error so can
;; handle successive errors, and that we can handle errors with CONDITION-CASE.
(defun pg-test-errors (con)
  (pg-cancel con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should-error (pg-exec con "SELECT * FROM"))
    (should (eql 42 (scalar "SELECT 42")))
    (should-error (pg-exec con "SELECT 42#"))
    (should (eql 9 (scalar "SELECT 4+5")))
    (should (eql 2 (condition-case nil
                       (pg-exec con "SELECT ###")
                     (pg-error 2))))
    ;; PostgreSQL should signal numerical overflow
    (should-error (pg-exec con "SELECT 2147483649::int4"))
    (should (eql -42 (scalar "SELECT -42")))
    (should-error (pg-exec con "SELECT 'foobles'::unexistingtype"))
    (should (eql -55 (scalar "SELECT -55")))))

;; Check our handling of NoticeMessage messages, and the correct operation of
;; `pg-handle-notice-functions'.
(defun pg-test-notice (con)
  (message "Testing handler functions for NOTICE messages")
  ;; The DROP TABLE will generate a NOTICE. We install a handler function that checks for the
  ;; name of the table in the NOTICE message (the message will be localized, but hopefully the
  ;; table name will always be present).
  (cl-flet ((deity-p (ntc) (should (cl-search "deity" (pgerror-message ntc)))))
    (let ((pg-handle-notice-functions (list #'deity-p)))
      (pg-exec con "DROP TABLE IF EXISTS deity"))))

;; Check handling of asynchronous notifications, as generated by LISTEN/NOTIFY. Note that this test
;; is not actually relying on any asynchronous functionality; the notification is received in
;; response to the dummy SELECT request.
(defun pg-test-notify (con)
  (message "Testing LISTEN/NOTIFY")
  (cl-flet ((notification-handler (channel payload)
              (message "Async notification on %s: %s" channel payload)))
    (pg-add-notification-handler con #'notification-handler)
    (pg-exec con "LISTEN yourheart")
    (pg-exec con "NOTIFY yourheart, 'foobles'")
    (pg-exec con "SELECT 'ignored'")
    (pg-exec con "NOTIFY yourheart, 'bazzles'")
    (sleep-for 10)
    (pg-exec con "SELECT 'ignored'")
    (pg-exec con "NOTIFY yourheart")
    (pg-exec con "SELECT 'ignored'")
    ;; The function pg_notify is an alternative to the LISTEN statement, and more flexible if your
    ;; channel name is determined by a variable.
    (pg-exec con "SELECT pg_notify('yourheart', 'leaving')")
    (pg-exec con "SELECT 'ignored'")
    (pg-exec con "UNLISTEN yourheart")
    (pg-exec con "NOTIFY yourheart, 'Et redit in nihilum quod fuit ante nihil.'")))


;; Only the superuser can issue a VACUUM. A bunch of NOTICEs will be emitted indicating this. This
;; test is not robust across PostgreSQL versions, however.
; (let ((notice-counter 0))
;   (let ((pg-handle-notice-functions (list (lambda (_n) (cl-incf notice-counter)))))
;     (pg-exec con "VACUUM")
;     (should (> notice-counter 0)))))


;; test of large-object interface. Note the use of with-pg-transaction
;; to wrap the requests in a BEGIN..END transaction which is necessary
;; when working with large objects.
(defun pg-test-lo-read (con)
  (with-pg-transaction con
    (let* ((oid (pg-lo-create con "rw"))
           (fd (pg-lo-open con oid "rw")))
      (message "==================================================")
      (pg-lo-write con fd "Hi there mate")
      (pg-lo-lseek con fd 3 0)         ; SEEK_SET = 0
      (unless (= 3 (pg-lo-tell con fd))
        (error "lo-tell test failed!"))
      (message "Read %s from lo" (pg-lo-read con fd 7))
      (message "==================================================")
      (pg-lo-close con fd)
      (pg-lo-unlink con oid))))

(defun pg-test-lo-import (con)
   (with-pg-transaction con
    (let ((oid (pg-lo-import con "/etc/group")))
      (pg-lo-export con oid "/tmp/group")
      (cond ((zerop (call-process "diff" nil nil nil "/tmp/group" "/etc/group"))
             (message "lo-import test succeeded")
             (delete-file "/tmp/group"))
            (t
             (message "lo-import test failed: check differences")
             (message "between files /etc/group and /tmp/group")))
      (pg-lo-unlink con oid))))

(defun pg-cleanup ()
  (interactive)
  (dolist (b (buffer-list))
    (when (string-match " \\*PostgreSQL\\*" (buffer-name b))
      (let ((p (get-buffer-process b)))
        (when p
          (delete-process p)))
      (kill-buffer b))))


(defun pg-bench ()
  (let* ((time (current-time))
         (_ (pg-test))
         (elapsed (float-time (time-since time))))
    (message "Emacs version %s: %s" (version) elapsed)))


;; EOF
