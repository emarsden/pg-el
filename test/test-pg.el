;;; Tests for the pg.el library   -*- coding: utf-8; -*-
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2022  Eric Marsden


(require 'cl-lib)
(require 'pg)
(require 'ert)

(defmacro with-pgtest-connection (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (or (string-to-number (getenv "PGEL_PORT")) 5432)))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port)
         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (or (string-to-number (getenv "PGEL_PORT")) 5432)))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port t)
        ,@body)))

(defmacro with-pgtest-connection-local (conn &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (or (string-to-number (getenv "PGEL_PORT")) 5432))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
    `(with-pg-connection-local ,conn (,path ,db ,user ,password)
        ,@body)))

(defun pg-test ()
  (cl-flet ((log-param-change (con name value)
               (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection conn
         (message "Running pg.el tests in %s against backend %s"
                  (version) (pg-backend-version conn)))))
  (message "Testing basic type parsing")
  (pg-test-basic)
  (message "Testing insertions...")
  (pg-test-insert)
  (message "Testing date routines...")
  (pg-test-date)
  (message "Testing numeric routines...")
  (pg-test-numeric)
  (message "Testing field extraction routines...")
  (pg-test-result)
  (pg-test-bytea)
  (pg-test-json)
  (pg-test-hstore)
  (message "Testing database creation")
  (pg-test-createdb)
  (pg-test-parameter-change-handlers)
  (message "Testing error handling")
  (pg-test-errors)
  (pg-test-notice)
  ;; (message "Testing large-object routines...")
  ;; (pg-test-lo-read)
  ;; (pg-test-lo-import)
  (message "Tests passed"))

(defun pg-test-tls ()
  (cl-flet ((log-param-change (con name value)
                              (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection conn
                              (message "Running pg.el tests in %s against backend %s"
                                       (version) (pg-backend-version conn)))))
  (message "Testing basic type parsing")
  (pg-test-basic)
  (message "Testing insertions...")
  (pg-test-insert)
  (message "Testing date routines...")
  (pg-test-date)
  (message "Testing numeric routines...")
  (pg-test-numeric)
  (message "Testing field extraction routines...")
  (pg-test-result)
  (pg-test-bytea)
  (pg-test-json)
  (pg-test-hstore)
  (message "Testing database creation")
  (pg-test-createdb)
  (pg-test-parameter-change-handlers)
  (message "Testing error handling")
  (pg-test-errors)
  (pg-test-notice)
  (message "Tests passed"))

;; Run tests over local Unix socket connection to backend
(defun pg-test-local ()
  (cl-flet ((log-param-change (con name value)
               (message "PG> backend parameter %s=%s" name value)))
    (let ((pg-parameter-change-functions (cons #'log-param-change pg-parameter-change-functions)))
      (with-pgtest-connection conn
         (message "Running pg.el tests in %s against backend %s"
                  (version) (pg-backend-version conn)))))
  (message "Testing basic type parsing")
  (pg-test-basic)
  (message "Testing insertions...")
  (pg-test-insert)
  (message "Testing date routines...")
  (pg-test-date)
  (message "Testing numeric routines...")
  (pg-test-numeric)
  (message "Testing field extraction routines...")
  (pg-test-result)
  (pg-test-bytea)
  (pg-test-json)
  (pg-test-hstore)
  (message "Testing database creation")
  (pg-test-createdb)
  (pg-test-parameter-change-handlers)
  (message "Testing error handling")
  (pg-test-errors)
  (pg-test-notice)
  (message "Tests passed"))

(defun pg-test-basic ()
  (with-pgtest-connection conn
    (cl-flet ((row (sql) (pg-result (pg-exec conn sql) :tuple 0)))
      (should (equal (list t nil) (row "SELECT true, false")))
      (should (equal (list 42) (row "SELECT 42")))
      (should (equal (list -1) (row "SELECT -1::integer")))
      (should (equal (list "hey" "Jude") (row "SELECT 'hey', 'Jude'")))
      (should (equal (list nil) (row "SELECT NULL")))
      (should (equal (list 1 nil "all") (row "SELECT 1,NULL,'all'")))
      ;; QuestDB doesn't clearly identify itself in its version string, and doesn't implement CHR()
      (unless (cl-search "Visual C++ build 1914" (pg-backend-version conn))
        (should (string= "Z" (car (row "SELECT chr(90)")))))
      (should (equal (list 12) (row "select length('(╯°□°)╯︵ ┻━┻')")))
      (should (eql nil (row " SELECT 3 where 1=0")))
      (should (string= "howdy" (car (row "SELECT 'howdy'::text"))))
      (should (string= "gday" (car (row "SELECT 'gday'::varchar(20)")))))))

(defun pg-test-insert ()
  (with-pgtest-connection conn
   (let ((res (list))
         (count 100))
     (when (member "count_test" (pg-tables conn))
       (pg-exec conn "DROP TABLE count_test"))
     (pg-exec conn "CREATE TABLE count_test(key int, val int)")
     (should (member "count_test" (pg-tables conn)))
     (should (member "val" (pg-columns conn "count_test")))
     (cl-loop for i from 1 to count
           for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                             i (* i i))
           do (pg-exec conn sql))
     (setq res (pg-exec conn "SELECT count(*) FROM count_test"))
     (should (= count (cl-first (pg-result res :tuple 0))))
     (setq res (pg-exec conn "SELECT sum(key) FROM count_test"))
     (should (= (cl-first (pg-result res :tuple 0))
                (/ (* count (1+ count)) 2)))
     (pg-exec conn "DROP TABLE count_test")
     (should (not (member "count_test" (pg-tables conn)))))))

;; Testing for the time handling routines. Expected output is something like (in buffer *Messages*,
;; or on the terminal if running the tests in batch mode)
;;
;; timestamp = (14189 17420)
;; abstime = (14189 17420)
;; time = 19:42:06
(defun pg-test-date ()
  (with-pgtest-connection conn
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0))))
      (let (res)
        (pg-exec conn "CREATE TABLE date_test(a timestamp, b time)")
        (pg-exec conn "INSERT INTO date_test VALUES "
                 "(current_timestamp, 'now')")
        (setq res (pg-exec conn "SELECT * FROM date_test"))
        (setq res (pg-result res :tuple 0))
        (message "timestamp = %s" (cl-first res))
        (message "time = %s" (cl-second res)))
      (pg-exec conn "DROP TABLE date_test")
      (should (equal (scalar "SELECT '2022-10-01'::date") (encode-time 0 0 0 1 10 2022)))
      (should (equal (scalar "SELECT 'PT42S'::interval") "00:00:42"))
      (should (equal (scalar "select '05:00'::time") "05:00:00"))
      (should (equal (scalar "SELECT '2001-02-03 04:05:06'::timestamp")
                     (encode-time 6 5 4 3 2 2001 nil t))))))

(defun pg-test-numeric ()
  (with-pgtest-connection conn
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0)))
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
      (unless (cl-search "CockroachDB" (pg-backend-version conn))
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
      (should (string= (scalar "SELECT lower('FÔÖÉ')") "fôöé"))
      (should (string= (scalar "SELECT lower('FÔ🐘💥bz')") "fô🐘💥bz"))
      (should (eql (scalar "SELECT ascii('a')") 97))
      (should (eql (length (scalar "SELECT repeat('Q', 5000)")) 5000))
      (should (string= (scalar "SELECT interval '1 day' + interval '3 days'") "4 days"))
      (should (eql (scalar "SELECT date '2001-10-01' - date '2001-09-28'") 3))
      ;; we are not parsing XML values
      (unless (or (cl-search "CockroachDB" (pg-backend-version conn))
                  (cl-search "-YB-" (pg-backend-version conn)))
        (should (string= (scalar "SELECT xmlforest('abc' AS foo, 123 AS bar)")
                         "<foo>abc</foo><bar>123</bar>"))))))


;; tests for BYTEA type (https://www.postgresql.org/docs/15/functions-binarystring.html)
(defun pg-test-bytea ()
  (with-pgtest-connection conn
     (pg-exec conn "CREATE TABLE byteatest(blob BYTEA, tag int)")
     (pg-exec conn "INSERT INTO byteatest VALUES('warning\\000'::bytea, 1)")
     (pg-exec conn "INSERT INTO byteatest VALUES('\\001\\002\\003'::bytea, 2)")
     (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0))))
       (should (equal (byte-to-string 0) (scalar "SELECT '\\000'::bytea")))
       (should (equal (byte-to-string ?') (scalar "SELECT ''''::bytea")))
       (should (equal "\336\255\276\357" (scalar "SELECT '\\xDEADBEEF'::bytea")))
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
         (pg-exec conn (format "INSERT INTO byteatest VALUES (decode('%s', 'base64'), 3)"
                               (base64-encode-string random-octets)))
         (should (equal random-octets (scalar "SELECT blob FROM byteatest WHERE tag=3")))))
     (pg-exec conn "DROP TABLE byteatest")))

(defun pg-test-array ()
  (with-pgtest-connection conn
     (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0)))
               (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
       (should (equal (vector 7 8) (scalar "SELECT ARRAY[7,8]")))
       (should (equal (vector 1234) (scalar "SELECT ARRAY[1234::int2]")))
       (should (equal (vector -3456) (scalar "SELECT ARRAY[-3456::int4]")))
       (should (equal (vector 9987) (scalar "SELECT ARRAY[9987::int8]")))
       (should (eql (vector 42) (scalar "SELECT array_agg(42)")))
       (should (equal (vector 45 67 89) (scalar "SELECT '{45,67,89}'::smallint[]")))
       (should (equal (vector t nil t nil t)
                      (scalar "SELECT '{true, false, true, false, true}'::bool[]")))
       (should (equal (vector ?A ?z ?5) (scalar "SELECT '{A,z,5}'::char[]")))
       (should (equal (vector "foo" "bar") (scalar "SELECT '{foo, bar}'::text[]")))
       (let ((vec (scalar "SELECT ARRAY[44.3, 8999.5]")))
         (should (equal 2 (length vec)))
         (should (approx= 44.3 (aref vec 0)))
         (should (approx= 899.5 (aref vec 1)))))))

  ;; eg  "SELECT CAST('{a,b,c}' AS CHAR[])")
  ;; select row(ARRAY[1,2,4,8])
  ;; select row(ARRAY[3.14::float])


;; https://www.postgresql.org/docs/15/functions-json.html
(defun pg-test-json ()
  (with-pgtest-connection conn
     (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0)))
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
           (should (eql 2 (aref (gethash "a" json) 2))))))))

(defun pg-test-hstore ()
  (with-pgtest-connection conn
     ;; We need to call this before using HSTORE datatypes to load the extension if necessary, and
     ;; to set up our parser support for the HSTORE type.
     (pg-hstore-setup conn)
     (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0))))
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
         (should (cl-find "boz" arr :test #'string=))))))


;; "SELECT xmlcomment("42") -> "<!--42-->"
(defun pg-test-xmlbinary ()
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
(defun pg-test-result ()
  (with-pgtest-connection conn
   (let ((r1 (pg-exec conn "CREATE TABLE resulttest (a int, b VARCHAR(4))"))
         (r2 (pg-exec conn "INSERT INTO resulttest VALUES (3, 'zae')"))
         (r3 (pg-exec conn "INSERT INTO resulttest VALUES (66, 'poiu')"))
         (r4 (pg-exec conn "SELECT * FROM resulttest"))
         (r5 (pg-exec conn "DROP TABLE resulttest"))
         (r6 (pg-exec conn "SELECT generate_series(1, 10)")))
     (message "==============================================")
     (message "status of CREATE is %s" (pg-result r1 :status))
     (message "status of INSERT is %s" (pg-result r2 :status))
     (message "oid of INSERT is %s"    (pg-result r2 :oid))
     (message "status of SELECT is %s" (pg-result r4 :status))
     (message "attributes of SELECT are %s" (pg-result r4 :attributes))
     (message "tuples of SELECT are %s" (pg-result r4 :tuples))
     (message "second tuple of SELECT is %s" (pg-result r4 :tuple 1))
     (message "status of DROP is %s" (pg-result r5 :status))
     (message "==============================================")
     (should (eql (length (pg-result r6 :tuples)) 10)))
   (let ((res (pg-exec conn "SELECT 1 UNION SELECT 2")))
     (should (equal '((1) (2)) (pg-result res :tuples))))
   (let ((res (pg-exec conn "SELECT 1,2,3,'soleil'")))
     (should (equal '(1 2 3 "soleil") (pg-result res :tuple 0))))
   (let ((res (pg-exec conn "SELECT 42 as z")))
     (should (string= "z" (caar (pg-result res :attributes)))))
   (let* ((res (pg-exec conn "SELECT 42 as z, 'bob' as bob"))
          (attr (pg-result res :attributes)))
     (should (string= "z" (caar attr)))
     (should (string= "bob" (caadr attr))))
   (pg-exec conn "DROP TYPE IF EXISTS FRUIT")
   (pg-exec conn "CREATE TYPE FRUIT AS ENUM('banana', 'orange', 'apple', 'pear')")
   (let* ((res (pg-exec conn "SELECT 'apple'::fruit"))
          (attr (pg-result res :attributes)))
     (should (string= "apple" (car (pg-result res :tuple 0))))
     (should (string= "fruit" (caar attr))))
   (let* ((res (pg-exec conn "SELECT 32 as éléphant"))
          (attr (pg-result res :attributes)))
     (should (string= "éléphant" (caar attr)))
     (should (eql 32 (car (pg-result res :tuple 0)))))))


(defun pg-test-createdb ()
  (with-pgtest-connection conn
    (when (member "pgeltestextra" (pg-databases conn))
       (pg-exec conn "DROP DATABASE pgeltestextra"))
    (pg-exec conn "CREATE DATABASE pgeltestextra")
    (should (member "pgeltestextra" (pg-databases conn)))
    ;; CockroachDB and YugabyteDB don't implement REINDEX
    (unless (or (cl-search "CockroachDB" (pg-backend-version conn))
                (cl-search "-YB-" (pg-backend-version conn)))
      (pg-exec conn "REINDEX DATABASE pgeltestdb"))
    (let* ((r (pg-exec conn "SHOW ALL"))
           (config (pg-result r :tuples)))
      (cl-loop for row in config
               when (string= "port" (car row))
               do (message "Connected to PostgreSQL on port %s" (cadr row))))
    (pg-exec conn "DROP DATABASE pgeltestextra")))


;; Test our support for handling ParameterStatus messages, via the pg-parameter-change-functions
;; variable. When we change the session timezone, the backend should send us a ParameterStatus
;; message with TimeZone=<new-value>.
(defun pg-test-parameter-change-handlers ()
  (message "Testing parameter-change-functions hook")
  (with-pgtest-connection conn
    (let ((handler-called nil))
      (cl-flet ((tz-handler (con name value)
                  (when (string= "TimeZone" name)
                    (setq handler-called t))))
        (cl-pushnew #'tz-handler pg-parameter-change-functions)
        ;; The backend will only send us a ParameterStatus message when the timezone changes, so
        ;; we make two changes to make sure at least one of them generates a ParameterStatus message.
        (pg-exec conn "SET SESSION TIME ZONE 'Europe/Paris'")
        (pg-exec conn "SET SESSION TIME ZONE 'America/Chicago'")
        (pg-exec conn "SELECT 42")
        (should (eql t handler-called))))))

;; Check that we raise errors when expected, that we resync with the backend after an error so can
;; handle successive errors, and that we can handle errors with CONDITION-CASE.
(defun pg-test-errors ()
  (with-pgtest-connection conn
    (pg-cancel conn)
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0))))
      (should-error (pg-exec conn "SELECT * FROM"))
      (should (eql 42 (scalar "SELECT 42")))
      (should-error (pg-exec conn "SELECT 42#"))
      (should (eql 9 (scalar "SELECT 4+5")))
      (should (eql 2 (condition-case nil
                         (pg-exec conn "SELECT ###")
                       (pg-error 2))))
      ;; PostgreSQL should signal numerical overflow
      (should-error (pg-exec conn "SELECT 2147483649::int4"))
      (should (eql -42 (scalar "SELECT -42")))
      (should-error (pg-exec conn "SELECT 'foobles'::unexistingtype"))
      (should (eql -55 (scalar "SELECT -55"))))))

;; Check our handling of NoticeMessage messages, and the correct operation of
;; `pg-handle-notice-functions'.
(defun pg-test-notice ()
  (with-pgtest-connection conn
     ;; The DROP TABLE will generate a NOTICE. We install a handler function that checks for the
     ;; name of the table in the NOTICE message (the message will be localized, but hopefully the
     ;; table name will always be present).
     (cl-flet ((deity-p (ntc) (should (cl-search "deity" (pgerror-message ntc)))))
       (let ((pg-handle-notice-functions (list #'deity-p)))
         (pg-exec conn "DROP TABLE IF EXISTS deity")))))


;; test of large-object interface. Note the use of with-pg-transaction
;; to wrap the requests in a BEGIN..END transaction which is necessary
;; when working with large objects.
(defun pg-test-lo-read ()
  (with-pgtest-connection conn
   (with-pg-transaction conn
    (let* ((oid (pg-lo-create conn "rw"))
           (fd (pg-lo-open conn oid "rw")))
      (message "==================================================")
      (pg-lo-write conn fd "Hi there mate")
      (pg-lo-lseek conn fd 3 0)           ; SEEK_SET = 0
      (unless (= 3 (pg-lo-tell conn fd))
        (error "lo-tell test failed!"))
      (message "Read %s from lo" (pg-lo-read conn fd 7))
      (message "==================================================")
      (pg-lo-close conn fd)
      (pg-lo-unlink conn oid)))))

(defun pg-test-lo-import ()
  (with-pgtest-connection conn
   (with-pg-transaction conn
    (let ((oid (pg-lo-import conn "/etc/group")))
      (pg-lo-export conn oid "/tmp/group")
      (cond ((zerop (call-process "diff" nil nil nil "/tmp/group" "/etc/group"))
             (message "lo-import test succeeded")
             (delete-file "/tmp/group"))
            (t
             (message "lo-import test failed: check differences")
             (message "between files /etc/group and /tmp/group")))
      (pg-lo-unlink conn oid)))))

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
