;;; Tests for the pg.el library
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
        (port (or (getenv "PGEL_PORT") 5432)))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port)
         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (or (getenv "PGEL_PORT") 5432)))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port t)
        ,@body)))

(defmacro with-pgtest-connection-local (conn &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (or (getenv "PGEL_PORT") 5432))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%d" port))))
    `(with-pg-connection-local ,conn (,path ,db ,user ,password)
        ,@body)))

(defun pg-test ()
  (with-pgtest-connection conn
   (message "Running pg.el tests against backend %s"
            (pg-backend-version conn))
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
   (message "Testing database creation")
   (pg-test-createdb)
   (pg-test-parameter-change-handlers)
   (message "Testing error handling")
   (pg-test-errors)
   ;; (message "Testing large-object routines...")
   ;; (pg-test-lo-read)
   ;; (pg-test-lo-import)
   (message "Tests passed")))

(defun pg-test-tls ()
  (with-pgtest-connection-tls conn
    (message "Running pg.el tests over TLS against backend %s"
             (pg-backend-version conn))
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
    (message "Testing database creation")
    (pg-test-createdb)
    (pg-test-parameter-change-handlers)
    (message "Testing error handling")
    (pg-test-errors)
    (message "Tests passed")))

;; Run tests over local Unix socket connection to backend
(defun pg-test-local ()
  (with-pgtest-connection-local conn
    (message "Running pg.el tests over Unix socket against backend %s"
             (pg-backend-version conn))
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
    (message "Testing database creation")
    (pg-test-createdb)
    (pg-test-parameter-change-handlers)
    (message "Testing error handling")
    (pg-test-errors)
    (message "Tests passed")))

(defun pg-test-basic ()
  (with-pgtest-connection conn
    (cl-flet ((row (sql) (pg-result (pg-exec conn sql) :tuple 0)))
      (should (equal (list t nil) (row "SELECT true, false")))
      (should (equal (list 42) (row "SELECT 42")))
      (should (equal (list "hey" "Jude") (row "SELECT 'hey', 'Jude'")))
      (should (equal (list nil) (row "SELECT NULL")))
      (should (equal (list 1 nil "all") (row "SELECT 1,NULL,'all'")))
      (should (string= "Z" (car (row "SELECT chr(90)"))))
      (should (string= "gday" (car (row "SELECT 'gday'::varchar(20)")))))))

(defun pg-test-insert ()
  (with-pgtest-connection conn
   (let ((res (list))
         (count 100))
     (pg-exec conn "CREATE TABLE count_test(key int, val int)")
     (cl-loop for i from 1 to count
           for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                             i (* i i))
           do (pg-exec conn sql))
     (setq res (pg-exec conn "SELECT count(val) FROM count_test"))
     (should (= count (cl-first (pg-result res :tuple 0))))
     (setq res (pg-exec conn "SELECT sum(key) FROM count_test"))
     (should (= (cl-first (pg-result res :tuple 0))
                (/ (* count (1+ count)) 2)))
     (pg-exec conn "DROP TABLE count_test"))))

;; Testing for the time handling routines. Expected output is
;; something like (in buffer *Messages*)
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
      (should (equal (scalar "SELECT '2001-02-03 04:05:06'::timestamp") (encode-time 6 5 4 3 2 2001 nil t))))))

(defun pg-test-numeric ()
  (with-pgtest-connection conn
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec conn sql) :tuple 0)))
              (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
      (should (eql -1 (scalar "SELECT '-1'::int")))
      (should (eql 128 (scalar "SELECT 128::int2")))
      (should (eql -128 (scalar "SELECT -128::int4")))
      (should (eql 66 (scalar "SELECT 66::int8")))
      (should (eql 42 (scalar "SELECT '42'::smallint")))
      (should (eql 66 (scalar "SELECT 66::money")))
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
      ;; TODO:  "SELECT 'NaN'::float8" is not being handled correctly, likewise "SELECT 'Infinity'::float8"
      (should (string= (scalar "SELECT 42::decimal::text") "42"))
      (should (eql (scalar "SELECT char_length('foo')") 3))
      (should (string= (scalar "SELECT lower('FOO')") "foo"))
      (should (string= (scalar "SELECT lower('FÔÖÉ💥')") "fôöé💥"))
      (should (eql (scalar "SELECT ascii('a')") 97))
      (should (eql (length (scalar "SELECT repeat('Q', 5000)")) 5000))
      (should (string= (scalar "SELECT interval '1 day' + interval '3 days'") "4 days"))
      (should (eql (scalar "SELECT date '2001-10-01' - date '2001-09-28'") 3))
      ;; we are not parsing XML values
      (unless (cl-search "CockroachDB" (pg-backend-version conn))
        (should (string= (scalar "SELECT xmlforest('abc' AS foo, 123 AS bar)") "<foo>abc</foo><bar>123</bar>"))))))


;; TODO: implement tests for BYTEA type (https://www.postgresql.org/docs/15/functions-binarystring.html)
(defun pg-test-bytea ()
  nil)

;; https://www.postgresql.org/docs/14/functions-json.html
(defun pg-test-json ()
  nil)


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
   (let* ((res (pg-exec conn "SELECT 32 as éléphant"))
          (attr (pg-result res :attributes)))
     (should (string= "éléphant" (caar attr)))
     (should (eql 32 (car (pg-result res :tuple 0)))))))


(defun pg-test-createdb ()
  (with-pgtest-connection conn
    (when (member "pgeltestextra" (pg-databases conn))
       (pg-exec conn "DROP DATABASE pgeltestextra"))
    (pg-exec conn "CREATE DATABASE pgeltestextra")
    (unless (cl-search "CockroachDB" (pg-backend-version conn))
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


(defun pg-test-errors ()
  (with-pgtest-connection conn
    (should-error (pg-exec conn "SELECT * FROM"))))

;; FIXME: should be able to condition-case on a pg-error condition
;; FIXME: should be able to handle two successive errors  (should-error (pg-exec conn "foobles"))))



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
