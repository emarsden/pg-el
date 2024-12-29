;;; Tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2022-2024  Eric Marsden
;; SPDX-License-Identifier: GPL-3.0-or-later


(require 'cl-lib)
(require 'pg)
(require 'pg-geometry)
(require 'pg-gis)
(require 'ert)


(setq debug-on-error t)


;; for performance testing
;; (setq process-adaptive-read-buffering nil)


(defmacro with-pgtest-connection (con &rest body)
  (cond ((getenv "PGURI")
         `(let ((,con (pg-connect/uri ,(getenv "PGURI"))))
            (unwind-protect
                (progn ,@body)
              (when ,con (pg-disconnect ,con)))))
        (t
         (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
                (user (or (getenv "PGEL_USER") "pgeltestuser"))
                (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
                (host (or (getenv "PGEL_HOSTNAME") "localhost"))
                (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
                (server-variant-str (getenv "PGEL_SERVER_VARIANT"))
                (server-variant (and server-variant-str
                                     (intern server-variant-str))))
           `(with-pg-connection ,con (,db ,user ,password ,host ,port nil ',server-variant)
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

;; Connect to the database presenting a client certificate as authentication
(defmacro with-pgtest-connection-client-cert (con &rest body)
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
               (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
               (cert (getenv "PGEL_CLIENT_CERT"))
               (key (getenv "PGEL_CLIENT_CERT_KEY")))
           `(progn
             (unless ,cert
               (error "Set $PGEL_CLIENT_CERT to point to file containing client certificate"))
             (unless ,key
               (error "Set $PGEL_CLIENT_CERT_KEY to point to file containing client certificate key"))
             (with-pg-connection ,con (,db ,user ,password ,host ,port '(:keylist ((,key ,cert))))
                                 ,@body))))))
(put 'with-pgtest-connection-client-cert 'lisp-indent-function 'defun)


(defmacro with-pgtest-connection-local (con &rest body)
  (cond ((getenv "PGURI")
         `(let ((,con (pg-connect/uri ,(getenv "PGURI"))))
            (unwind-protect
                (progn ,@body)
              (when ,con (pg-disconnect ,con)))))
        (t
         (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
                (user (or (getenv "PGEL_USER") "pgeltestuser"))
                (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
                (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
                (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
           `(with-pg-connection-local ,con (,path ,db ,user ,password)
               ,@body)))))
(put 'with-pg-connection-local 'lisp-indent-function 'defun)

;; Some utility functions to allow us to skip some tests that we know fail on some PostgreSQL
;; versions or hosters or semi-compatible implementations
(defun pg-test-is-cratedb (con)
  (eq (pgcon-server-variant con) 'cratedb))

;; Nothing is present in the server-version string. But it has some unusual behaviour; for example
;; not respecting the table_owner setting (the value returned by pg-table-owner is not the same as
;; the username we use to connect).
(defun pg-test-is-xata (con)
  (eq (pgcon-server-variant con) 'xata))

(defun pg-test-is-cockroachdb (con)
  (eq (pgcon-server-variant con) 'cockroachdb))

(defun pg-test-is-yugabyte (con)
  (cl-search "-YB-" (pg-backend-version con)))

;; Google Spanner (or at least, the emulator which is what we have tested) pretends to be
;; "PostgreSQL 14.1", but doesn't implement some quite basic PostgreSQL functionality, such as the
;; CHR() function.
(defun pg-test-is-spanner (con)
  ;; See function pg-test-note-param-change below; this is an ugly hack!
  (gethash "is-spanner-p" (pgcon-prepared-statement-cache con) nil))

   ;; QuestDB doesn't clearly identify itself in its version string, and doesn't implement CHR()
(defun pg-test-is-questdb (con)
  (cl-search "Visual C++ build 1914" (pg-backend-version con)))

(defun pg-test-is-ydb (con)
  (gethash "is-ydb-p"  (pgcon-prepared-statement-cache con) nil))

(defun pg-test-is-immudb (con)
  (cl-search "implemented by immudb" (pg-backend-version con)))

(defun pg-test-is-greptimedb (con)
  (cl-search "-greptime-" (pg-backend-version con)))


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
      (pg-disconnect con)))
  ;; Now testing various environment variables. For libpq the recognized names are in
  ;; https://www.postgresql.org/docs/current/libpq-envars.html
  (dolist (v (list "postgresql://pgeltestuser@localhost/pgeltestdb?application_name=testingtesting"
                   "postgres://pgeltestuser@localhost/pgeltestdb?application_name=testingtesting"
                   "postgres://pgeltestuser@localhost:5432/pgeltestdb"
                   "postgres://pgeltestuser@localhost:5432/pgeltestdb?sslmode=prefer"
                   "postgres://pgeltestuser@%2Fvar%2Frun%2Fpostgresql%2F.s.PGSQL.5432/pgeltestdb"))
    (setenv "PGPASSWORD" "pgeltest")
    (let ((con (pg-connect/uri v)))
      (should (process-live-p (pgcon-process con)))
      (pg-disconnect con))))

(defun pg-run-tests (con)
  (pg-enable-query-log con)
  (message "Backend major-version is %s" (pgcon-server-version-major con))
  (message "Detected backend variant: %s" (pgcon-server-variant con))
  (unless (member (pgcon-server-variant con)
                  '(cockroachdb cratedb yugabyte ydb xata greptimedb risingwave clickhouse))
    (when (> (pgcon-server-version-major con) 11)
      (let* ((res (pg-exec con "SELECT current_setting('ssl_library')"))
             (row (pg-result res :tuple 0)))
        (message "Backend compiled with SSL library %s" (cl-first row)))))
  (unless (member (pgcon-server-variant con)
                  '(questdb cratedb ydb xata greptimedb risingwave clickhouse))
    (let* ((res (pg-exec con "SHOW ssl"))
           (row (pg-result res :tuple 0)))
      (message "PostgreSQL connection TLS: %s" (cl-first row))))
  (message "List of schemas in db: %s" (pg-schemas con))
  (message "List of tables in db: %s" (pg-tables con))
  (when (eq 'orioledb (pgcon-server-variant con))
    (pg-exec con "CREATE EXTENSION orioledb"))
  (unless (member (pgcon-server-variant con) '(clickhouse))
    (pg-setup-postgis con))
  (unless (member (pgcon-server-variant con) '(clickhouse))
    (pg-vector-setup con))
  (message "Testing basic type parsing")
  (pg-test-basic con)
  (message "Testing insertions...")
  (pg-test-insert con)
  (unless (or (pg-test-is-cratedb con)
              (version< emacs-version "29.1"))
    (message "Testing date routines...")
    (pg-test-date con))
  (message "Testing timezone handling ...")
  (pg-run-tz-tests con)
  (message "Testing numeric routines...")
  (pg-test-numeric con)
  (unless (or (pg-test-is-xata con)
              (pg-test-is-cratedb con)
              (pg-test-is-ydb con))
    (pg-test-numeric-range con))
  (when (>= emacs-major-version 28)
    (message "Testing prepared statements")
    (pg-test-prepared con)
    (pg-test-prepared/multifetch con)
    (pg-test-insert/prepared con)
    (pg-test-ensure-prepared con))
  (unless (or (pg-test-is-xata con)
              (pg-test-is-cratedb con))
    (pg-test-collation con))
  (unless (pg-test-is-xata con)
    (pg-test-xml con))
  (pg-test-uuid con)
  (message "Testing field extraction routines...")
  (pg-test-result con)
  (unless (pg-test-is-xata con)
    (pg-test-cursors con))
  ;; CrateDB does not support the BYTEA type (!).
  (unless (pg-test-is-cratedb con)
    (pg-test-bytea con))
  (pg-test-sequence con)
  (pg-test-array con)
  ;; Yugabyte does not support the "GENERATED ALWAYS AS" type columns
  (unless (pg-test-is-yugabyte con)
    (pg-test-metadata con))
  (unless (pg-test-is-xata con)
    (pg-test-schemas con))
  (pg-test-enums con)
  (pg-test-json con)
  (pg-test-server-prepare con)
  (pg-test-hstore con)
  ;; Xata doesn't support extensions, but doesn't signal an SQL error when we attempt to load the
  ;; pgvector extension, so our test fails despite being intended to be robust.
  (unless (pg-test-is-xata con)
    (pg-test-vector con)
    (pg-test-tsvector con)
    (pg-test-geometric con)
    (pg-test-gis con))
  (message "Testing COPY...")
  (unless (or (pg-test-is-spanner con)
              (pg-test-is-ydb con))
    (pg-test-copy con)
    (pg-test-copy-large con))
  (message "Testing database creation")
  ;; Apparently Xata does not support CREATE DATABASE
  (unless (pg-test-is-xata con)
    (pg-test-createdb con)
    (message "Testing unicode names for database, tables, columns")
    (pg-test-unicode-names con))
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


(defun pg-test-note-param-change (con name value)
  (message "PG> backend parameter %s=%s" name value)
  (when (and (string= "session_authorization" name)
             (string-prefix-p "xata" value))
    ;; This is a rather rude and ugly way of hiding some private information in the PostgreSQL
    ;; connection struct.
    (puthash "is-xata-p" t (pgcon-prepared-statement-cache con)))
  ;; Google Spanner (or at least the emulator) using PGAdapter for PostgreSQL wire protocol support
  (when (and (string= "session_authorization" name)
             (string= "PGAdapter" value))
    (puthash "is-spanner-p" t (pgcon-prepared-statement-cache con)))
  (when (and (string= "server_version" name)
             (cl-search "ydb stable" value))
    (setf (pgcon-timeout con) 50)
    (puthash "is-ydb-p" t (pgcon-prepared-statement-cache con))))


(defun pg-test ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection con
       (message "Running pg.el tests in %s against backend %s"
                (version) (pg-backend-version con))
       (pg-run-tests con))))

(defun pg-test-tls ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection-tls con
       (message "Running pg.el tests in %s against backend %s"
                (version) (pg-backend-version con))
       (pg-run-tests con))))

(defun pg-test-client-cert ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection-client-cert con
       (message "Running pg.el tests with client cert in %s against backend %s"
                (version) (pg-backend-version con))
       (pg-run-tests con))))

;; Run tests over local Unix socket connection to backend
(defun pg-test-local ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection-local conn
       (message "Running pg.el tests in %s against backend %s"
                (version) (pg-backend-version conn))
       (pg-run-tests conn))))

;; Simple connect and list tables test on a public RNAcentral PostgreSQL server hosted at ebi.ac.uk, see
;;  https://rnacentral.org/help/public-database.
(defun pg-test-ebiacuk ()
  (let ((con (pg-connect/uri "postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk/pfmegrnargs")))
    (message "Connected to %s, %s"
             (cl-prin1-to-string con)
             (pg-backend-version con))
    (dolist (table (pg-tables con))
      (message "  Table: %s" table))))



(defun pg-test-prepared (con)
  (cl-labels ((row (query args) (pg-result (pg-exec-prepared con query args) :tuple 0))
              (scalar (query args) (car (row query args)))
              (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (should (equal 42 (scalar "SELECT 42" (list))))
    (should (approx= 42.0 (scalar "SELECT 42.00" (list))))
    (should (equal nil (scalar "SELECT NULL" (list))))
    (unless (pg-test-is-ydb con)
      (should (equal nil (scalar "" (list)))))
    (unless (pg-test-is-cratedb con)
      (let ((bv1 (make-bool-vector 1 nil))
            (bv2 (make-bool-vector 1 t)))
        (should (equal bv1 (scalar "SELECT $1::bit" `((,bv1 . "bit")))))
        (should (equal bv1 (scalar "SELECT $1::varbit" `((,bv1 . "varbit")))))
        (should (equal bv2 (scalar "SELECT $1::bit" `((,bv2 . "bit")))))
        (should (equal bv2 (scalar "SELECT $1::varbit" `((,bv2 . "varbit")))))
        (should (equal bv1 (scalar "SELECT $1" `((,bv1 . "bit")))))
        (should (equal bv1 (scalar "SELECT $1" `((,bv1 . "varbit")))))
        (should (equal bv2 (scalar "SELECT $1" `((,bv2 . "bit")))))
      (should (equal bv2 (scalar "SELECT $1" `((,bv2 . "varbit"))))))
      ;; Now some bitvectors of length > 1, so shouldn't use the "bit" type which is interpreted as
      ;; bit(1).
      (let ((bv1 (bool-vector t nil t t t t))
            (bv2 (bool-vector t nil t t nil nil t t t t nil t)))
        (should (equal bv1 (scalar "SELECT $1" `((,bv1 . "varbit")))))
        (should (equal bv1 (scalar "SELECT $1::varbit" `((,bv1 . "varbit")))))
        (should (equal bv2 (scalar "SELECT $1" `((,bv2 . "varbit")))))
        (should (equal bv2 (scalar "SELECT $1::varbit" `((,bv2 . "varbit")))))
        (should (equal (bool-vector-intersection bv2 bv2)
                       (scalar "SELECT $1 & $2" `((,bv2 . "varbit") (,bv2 . "varbit")))))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int2")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int2")))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int4")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int4")))))
    (should (eql 42 (scalar "SELECT $1 + 1" '((41 . "int8")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "int8")))))
    (should (approx= -55.0 (scalar "SELECT $1" '((-55.0 . "float4")))))
    (should (approx= -55.0 (scalar "SELECT $1" '((-55.0 . "float8")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float4")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 85.0" '((-43.0 . "float4")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float8")))))
    (should (approx= 42.0 (scalar "SELECT $1 + 85" '((-43.0 . "float8")))))
    (unless (pg-test-is-cratedb con)
      ;; CrateDB returns an incorrect value ?8 here
      (should (eql ?Q (scalar "SELECT $1" '((?Q . "char"))))))
    (should (equal (list t nil) (row "SELECT $1, $2" '((t . "bool") (nil . "bool")))))
    (should (eql nil (scalar "SELECT $1 WHERE 0=1" '((42 . "int4")))))
    (should (string= "foobles" (scalar "SELECT $1" '(("foobles" . "text")))))
    (should (string= "foobles/var" (scalar "SELECT $1" '(("foobles/var" . "varchar")))))
    (should (string= "çéà" (scalar "SELECT $1::text" '(("çéà" . "text")))))
    (should (string= "fooblé" (scalar "SELECT $1" '(("fooblé" . "text")))))
    (should (string= "Bîzzlô⚠️" (scalar "SELECT $1" '(("Bîzzlô⚠️" . "varchar")))))
    (should (string= "foobles" (scalar "SELECT $1 || $2" '(("foo" . "text") ("bles" . "text")))))
    (unless (or (pg-test-is-cratedb con)
                (zerop (scalar "SELECT COUNT(*) FROM pg_collation WHERE collname='fr_FR'" nil)))
      (should (string= "12 foé£èüñ¡" (scalar "SELECT lower($1) COLLATE \"fr_FR\"" '(("12 FOÉ£ÈÜÑ¡" . "text"))))))
    (should (equal "00:00:12" (scalar "SELECT $1::interval" '(("PT12S" . "text")))))
    (should (equal -1 (scalar "SELECT $1::int" '((-1 . "int4")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float4" '((1.0e+INF . "float4")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float8" '((1.0e+INF . "float8")))))
    (should (eql 0.0e+NaN (scalar "SELECT $1::float4" '((0.0e+NaN . "float4")))))
    (should (eql 0.0e+NaN (scalar "SELECT $1::float8" '((0.0e+NaN . "float8")))))
    ;; CrateDB does not support the BYTEA type.
    (unless (pg-test-is-cratedb con)
      (should (equal (byte-to-string 0)
                     (scalar "SELECT $1::bytea" '(("\\000" . "text")))))
      (should (equal (byte-to-string 0)
                     (scalar "SELECT $1" `((,(byte-to-string 0) . "bytea")))))
      (should (equal (decode-hex-string "DEADBEEF")
                     (scalar "SELECT $1::bytea" '(("\\xDEADBEEF" . "text")))))
      (should (equal (decode-hex-string "DEADBEEF")
                     (scalar "SELECT $1" `((,(decode-hex-string "DEADBEEF") . "bytea"))))))
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
    (let ((ht (make-hash-table)))
      (puthash "biz" 45 ht)
      (puthash "boz" -5.5 ht)
      (puthash "comment" "good stuff" ht)
      (let* ((res (pg-exec-prepared con "SELECT $1->'biz'" `((,ht . "json"))))
             (row (pg-result res :tuple 0)))
        (should (eql 45 (cl-first row)))))
    (let ((ht (make-hash-table)))
      (puthash "biz" 45 ht)
      (puthash "boz" -5.5 ht)
      (puthash "comment" "good stuff" ht)
      (let* ((res (pg-exec-prepared con "SELECT $1 - 'boz'" `((,ht . "jsonb"))))
             (row (pg-result res :tuple 0)))
        (should (string= (gethash "comment" (cl-first row)) "good stuff"))))
    (when (pg-hstore-setup con)
      (let ((hs (scalar "SELECT $1::hstore" '(("a=>1,b=>2" . "text")))))
        (should (string= "1" (gethash "a" hs)))
        (should (eql 2 (hash-table-count hs))))
      (let ((ht (make-hash-table :test #'equal)))
        ;; HSTORE only supports string keys and values
        (puthash "biz" "baz" ht)
        (puthash "foo" "bar" ht)
        (puthash "more" "than" ht)
        (let* ((res (pg-exec-prepared con "SELECT $1 ? 'foo'" `((,ht . "hstore"))))
               (row (pg-result res :tuple 0)))
          (should (eql (cl-first row) t)))
        (let* ((res (pg-exec-prepared con "SELECT $1 - 'more'::text" `((,ht . "hstore"))))
               (row (pg-result res :tuple 0)))
          (should (hash-table-p (cl-first row)))
          (should (eql 2 (hash-table-count (cl-first row)))))))
    ;; Little Bobby Tables
    (pg-exec con "DROP TABLE IF EXISTS students")
    (pg-exec con "CREATE TABLE students(name TEXT, age INT)")
    (let* ((bobby "Robert'); DROP TABLE students;--")
           (res (pg-exec-prepared con "INSERT INTO students(name) VALUES ($1)"
                                  `((,bobby . "text"))))
           (count (cl-first (row "SELECT COUNT(*) FROM students" nil)))
           (name (cl-first (row "SELECT name FROM students LIMIT 1" nil))))
      (should (eql 1 count))
      (should (cl-search "Robert" name)))
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
  (cl-labels ((row (sql) (pg-result (pg-exec con sql) :tuple 0))
              (scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (should (equal (list 42) (row "SELECT 42")))
    (should (equal (list t) (row "SELECT true")))
    (unless (pg-test-is-immudb con)
      (should (equal (list t nil) (row "SELECT true, false"))))
    (should (eql -1 (scalar "SELECT -1::integer")))
    (should (eql 66 (scalar "SELECT 66::int2")))
    (should (eql -66 (scalar "SELECT -66::int2")))
    (should (eql 44 (scalar "SELECT 44::int4")))
    (should (eql -44 (scalar "SELECT -44::int4")))
    (should (eql 12345 (scalar "SELECT 12345::int8")))
    (should (eql -12345 (scalar "SELECT -12345::int8")))
    (should (eql nil (scalar "SELECT '0'::boolean")))
    (should (equal (list "hey" "Jude") (row "SELECT 'hey', 'Jude'")))
    (should (eql nil (scalar "SELECT NULL")))
    ;; This leads to a timeout with YDB
    (unless (pg-test-is-ydb con)
      (should (equal nil (row ""))))
    (unless (pg-test-is-cratedb con)
      (should (equal nil (row "-- comment"))))
    (should (equal (list 1 nil "all") (row "SELECT 1,NULL,'all'")))
    (unless (or (pg-test-is-questdb con)
                (pg-test-is-spanner con))
      (should (string= "Z" (scalar "SELECT chr(90)"))))
    (should (eql 12 (scalar "SELECT length('(╯°□°)╯︵ ┻━┻')")))
    (should (string= "::!!::" (scalar "SELECT '::!!::'::varchar")))
    (should (string= "éàç⟶∪" (scalar "SELECT 'éàç⟶∪'")))
    (let* ((res (pg-exec con "SELECT 42 as éléphant"))
           (col1 (cl-first (pg-result res :attributes))))
      (should (string= "éléphant" (cl-first col1))))
    ;; Note that we need to escape the ?\ character in an elisp string by repeating it.
    ;; CrateDB does not support the BYTEA type.
    (unless (pg-test-is-cratedb con)
      (should (eql 3 (length (scalar "SELECT '\\x123456'::bytea"))))
      (should (string= (string #x12 #x34 #x56) (scalar "SELECT '\\x123456'::bytea"))))
    (unless (pg-test-is-spanner con)
      (should (eql nil (row " SELECT 3 WHERE 1=0"))))
    (unless (or (pg-test-is-cratedb con)
                (pg-test-is-spanner con)
                (pg-test-is-ydb con))
      ;; these are row expressions, not standard SQL
      (should (string= (scalar "SELECT (1,2)") "(1,2)"))
      (should (string= (scalar "SELECT (null,1,2)") "(,1,2)")))
    (unless (eq 'risingwave (pgcon-server-variant con))
      (should (string= "foo\nbar" (scalar "SELECT $$foo
bar$$"))))
    (should (string= "foo\tbar" (scalar "SELECT 'foo\tbar'")))
    (should (string= "abcdef" (scalar "SELECT 'abc' || 'def'")))
    (should (string= "howdy" (scalar "SELECT 'howdy'::text")))
    ;; RisingWave does not support the VARCHAR(N) syntax.
    (unless (eq 'risingwave (pgcon-server-variant con))
      (should (string= "gday" (scalar "SELECT 'gday'::varchar(20)"))))
    ;; CockroachDB is returning these byteas in a non-BYTEA format so they are twice as long as
    ;; expected. CrateDB does not implement the sha256 and sha512 functions.
    (unless (or (pg-test-is-cockroachdb con)
                (pg-test-is-cratedb con))
      (should (eql 32 (length (scalar "SELECT sha256('foobles')"))))
      (should (eql 64 (length (scalar "SELECT sha512('foobles')")))))
    (unless (pg-test-is-spanner con)
      (should (string= (md5 "foobles") (scalar "SELECT md5('foobles')"))))
    (let* ((res (pg-exec con "SELECT 11 as bizzle, 15 as bazzle"))
           (attr (pg-result res :attributes))
           (col1 (cl-first attr))
           (col2 (cl-second attr))
           (row (pg-result res :tuple 0)))
      (should (eql 1 (length (pg-result res :tuples))))
      (should (eql 11 (cl-first row)))
      (should (eql 15 (cl-second row)))
      (should (string= "bizzle" (cl-first col1)))
      (should (string= "bazzle" (cl-first col2))))
    ;; This setting defined in PostgreSQL v8.2. A value of 120007 means major version 12, minor
    ;; version 7. The value in pgcon-server-version-major is obtained by parsing the server_version
    ;; string sent by the backend on startup. Not all servers return a value for this (for example
    ;; xata.sh servers return an empty string).
    (unless (pg-test-is-ydb con)
      (let* ((version-str (car (row "SELECT current_setting('server_version_num')")))
             (version-num (and version-str (cl-parse-integer version-str))))
        (if version-str
            (should (eql (pgcon-server-version-major con)
                         (/ version-num 10000)))
          (message "This PostgreSQL server doesn't support current_setting('server_version_num')"))))))


(defun pg-test-insert (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (let ((count 100))
      (when (member "count_test" (pg-tables con))
        (pg-exec con "DROP TABLE count_test"))
      (pg-exec con
               (format "CREATE TABLE count_test(key INT PRIMARY KEY, val INT) %s"
                       (if (eq 'orioledb (pgcon-server-variant con))
                           " USING orioledb"
                         "")))
      (should (member "count_test" (pg-tables con)))
      (should (member "val" (pg-columns con "count_test")))
      (unless (member (pgcon-server-variant con) '(cratedb xata ydb))
        (let ((user (or (nth 4 (pgcon-connect-info con))
                        "pgeltestuser"))
              (owner (pg-table-owner con "count_test")))
          ;; Some hosted PostgreSQL servers that require you to use a username of the form
          ;; myuser@the-hostname only return "myuser" from pg-table-owner.
          (unless (cl-search "@" user)
            (should (string= user owner))
            (should (string= user (pg-table-owner con (make-pg-qualified-name :name "count_test"))))
            (should (string= user (pg-table-owner con "count_test"))))))
      (unless (member (pgcon-server-variant con) '(ydb cratedb risingwave))
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
        (should (cl-search "cubed" (pg-table-comment con "count_test"))))
      (cl-loop for i from 1 to count
               for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                                   i (* i i))
                 do (pg-exec con sql))
        (should (eql count (scalar "SELECT count(*) FROM count_test")))
        (should (eql (/ (* count (1+ count)) 2) (scalar "SELECT sum(key) FROM count_test")))
        (pg-exec con "DROP TABLE count_test")
        (should (not (member "count_test" (pg-tables con)))))
      ;; Test for specific bugs when we have a table name and column names of length 1 (could be
      ;; interpreted as a character rather than as a string).
      (pg-exec con "DROP TABLE IF EXISTS w")
      (pg-exec con "CREATE TABLE w(i SERIAL PRIMARY KEY, v TEXT)")
      (unless (member (pgcon-server-variant con) '(ydb))
        (setf (pg-table-comment con "w") "c"))
      (should (stringp (pg-table-owner con "w")))
      (pg-exec con "INSERT INTO w(v) VALUES ('s')")
      (pg-exec con "INSERT INTO w(v) VALUES ('é')")
      (pg-exec-prepared con "INSERT INTO w(v) VALUES($1)" `(("a" . "text")))
      (let ((res (pg-exec con "SELECT * FROM w")))
        (should (eql 3 (length (pg-result res :tuples)))))
      (pg-exec con "DROP TABLE w")))

(defun pg-test-insert/prepared (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (let ((count 100))
      (when (member "count_test" (pg-tables con))
        (pg-exec con "DROP TABLE count_test"))
      (pg-exec con "CREATE TABLE count_test(key INT PRIMARY KEY, val INT)")
      (should (member "count_test" (pg-tables con)))
      (should (member "val" (pg-columns con "count_test")))
      (pg-exec con "TRUNCATE TABLE count_test")
      (dotimes (i count)
        (pg-exec-prepared con "INSERT INTO count_test VALUES($1, $2)"
                          `((,i . "int4") (,(* i i) . "int4"))))
      (should (eql count (scalar "SELECT COUNT(*) FROM count_test")))
      (should (eql (/ (* (1- count) count) 2) (scalar "SELECT sum(key) FROM count_test")))
      (pg-exec con "DROP TABLE count_test")
      (should (not (member "count_test" (pg-tables con)))))))

;; Check the mixing of prepared queries, cached prepared statements, normal simple queries, to check
;; that the cache works as expected and that the backend retains prepared statements. TODO: should
;; add here different PostgreSQL connections to the test, to ensure that the caches are not being
;; mixed up.
;;
;; We are seeing problems with Tembo in the mixing of prepared statements and normal statements.
(defun pg-test-ensure-prepared (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0)))
            (pfp (ps-name args)
              (let ((res (pg-fetch-prepared con ps-name args)))
                (cl-first (pg-result res :tuple 0)))))
    (pg-exec con "DROP TABLE IF EXISTS prep")
    (pg-exec con "CREATE TABLE prep(a INTEGER, b INTEGER)")
    (dotimes (i 10)
      (pg-exec-prepared con "INSERT INTO prep VALUES($1, $2)"
                        `((,i . "int4") (,(* i i) . "int4"))))
    (should (eql 10 (scalar "SELECT COUNT(*) FROM prep")))
    (let* ((ps1 (pg-ensure-prepared-statement
                 con "PGT-count1" "SELECT COUNT(*) FROM prep" nil))
           (ps2 (pg-ensure-prepared-statement
                 con "PGT-count2" "SELECT COUNT(*) FROM prep WHERE a >= $1" '("int4")))
           (ps3 (pg-ensure-prepared-statement
                 con "PGT-count3" "SELECT COUNT(*) FROM prep WHERE a + b >= $1" '("int4"))))
      (should (eql 10 (scalar "SELECT COUNT(*) FROM prep")))
      (should (eql 10 (pfp ps1 nil)))
      (should (eql 10 (pfp ps2 `((0 . "int4")))))
      (should (eql 10 (pfp ps3 `((0 . "int4")))))
      (should (eql 10 (scalar "SELECT COUNT(*) FROM prep")))
      (should (eql 10 (pfp ps2 `((0 . "int4")))))
      (should (eql 10 (scalar "SELECT COUNT(*) FROM prep WHERE b >= 0")))
      (dotimes (i 1000)
        (let ((v (pcase (random 4)
                   (0 (scalar "SELECT COUNT(*) FROM prep"))
                   (1 (pfp ps1 nil))
                   (2 (pfp ps2 `((0 . "int4"))))
                   (3 (pfp ps3 `((0 . "int4")))))))
          (should (eql v 10)))))
    (pg-exec con "DROP TABLE prep")))


;; Testing for the date/time handling routines.
(defun pg-test-date (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (with-environment-variables (("TZ" "Europe/Berlin"))
      (pg-exec con "SET TimeZone = 'Europe/Berlin'")
      (pg-exec con "DROP TABLE IF EXISTS date_test")
      (pg-exec con "CREATE TABLE date_test(id integer, ts timestamp, tstz timestamptz, t time, ttz timetz, d date)")
      (unwind-protect
          (progn
            (pg-exec con "INSERT INTO date_test(id, ts, tstz, t, ttz, d) VALUES "
                     "(1, current_timestamp, current_timestamp, 'now', 'now', current_date)")
            (let* ((res (pg-exec con "SELECT * FROM date_test"))
                   (row (pg-result res :tuple 0)))
              (message "timestamp = %s" (nth 1 row))
              (message "timestamptz = %s" (nth 2 row))
              (message "time = %s" (nth 3 row))
              (message "timetz = %s" (nth 4 row))
              (message "date = %s" (nth 5 row)))
            (pg-exec-prepared con "INSERT INTO date_test(id, ts, tstz, t, ttz, d) VALUES(2, $1, $2, $3, $4, $5)"
                              `((,(pg-isodate-without-timezone-parser "2024-04-27T11:34:42" nil) . "timestamp")
                                (,(pg-isodate-with-timezone-parser "2024-04-27T11:34:42.789+11" nil) . "timestamptz")
                                ("11:34" . "time")
                                ("16:55.33456+11" . "timetz")
                                (,(pg-date-parser "2024-04-27" nil) . "date")))
            (should (eql 2 (scalar "SELECT COUNT(*) FROM date_test"))))
        (pg-exec con "DROP TABLE date_test"))
      ;; this fails on CockroachDB
      (unless (pg-test-is-cockroachdb con)
        (should (equal (scalar "SELECT 'allballs'::time") "00:00:00")))
      (should (equal (scalar "SELECT '2022-10-01'::date")
                     (encode-time (list 0 0 0 1 10 2022))))
      ;; When casting to DATE, the time portion is truncated
      (should (equal (scalar "SELECT '2063-03-31T22:13:02'::date")
                     (encode-time (list 0 0 0 31 3 2063))))
      ;; Here the hh:mm:ss are taken into account.
      (should (equal (scalar "SELECT '2063-03-31T22:13:02'::timestamp")
                     (encode-time (list 2 13 22 31 3 2063 nil -1 'wall))))
      (should (equal (scalar "SELECT '2010-04-05 14:42:21'::timestamp with time zone")
                     ;; SECOND MINUTE HOUR DAY MONTH YEAR IGNORED DST ZONE
                     ;; Passing ZONE=nil means using Emacs' interpretation of local time
                     (encode-time (list 21 42 14 5 4 2010 nil -1 nil))))
      (should (equal (scalar "SELECT '2010-04-05 14:42:21'::timestamp without time zone")
                     (encode-time (list 21 42 14 5 4 2010 nil -1 'wall))))
      (should (equal (scalar "SELECT 'PT42S'::interval") "00:00:42"))
      (should (equal (scalar "SELECT 'PT3H4M42S'::interval") "03:04:42"))
      (should (equal (scalar "select '05:00'::time") "05:00:00"))
      (should (equal (scalar "SELECT '04:15:31.445+05'::timetz") "04:15:31.445+05"))
      (should (equal (scalar "SELECT '2001-02-03 04:05:06'::timestamp")
                     (encode-time (list 6 5 4 3 2 2001 nil -1 nil)))))))

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
    (should (eql 123 (scalar "SELECT 123::oid")))
    ;; CrateDB doesn't support casting integers to bits.
    (unless (pg-test-is-cratedb con)
      (should (equal (make-bool-vector 1 nil) (scalar "SELECT 0::bit")))
      (should (equal (make-bool-vector 1 t) (scalar "SELECT 1::bit")))
      (should (equal (make-bool-vector 8 t) (scalar "SELECT CAST(255 as bit(8))")))
      (let ((bv (scalar "SELECT CAST(32 as BIT(16))")))
        (should (eql nil (aref bv 0)))
        (should (eql nil (aref bv 3)))
        (should (eql t (aref bv 10)))
        (should (eql nil (aref bv 14)))))
    ;; Emacs version prior to 27 can't coerce to bool-vector type
    (when (> emacs-major-version 26)
      (should (equal (cl-coerce (vector t nil t nil) 'bool-vector)
                     (scalar "SELECT '1010'::bit(4)")))
      (should (equal (cl-coerce (vector t nil nil t nil nil nil) 'bool-vector)
                     (scalar "SELECT b'1001000'")))
      (unless (pg-test-is-cratedb con)
        (should (equal (cl-coerce (vector t nil t t t t) 'bool-vector)
                       (scalar "SELECT '101111'::varbit(6)")))))
    ;; (should (eql 66 (scalar "SELECT 66::money")))
    (should (eql (scalar "SELECT floor(42.3)") 42))
    (unless (pg-test-is-ydb con)
      (should (eql (scalar "SELECT trunc(43.3)") 43))
      (should (eql (scalar "SELECT trunc(-42.3)") -42)))
    (unless (pg-test-is-cockroachdb con)
      (should (approx= (scalar "SELECT log(100)") 2))
      ;; bignums only supported from Emacs 27.2 onwards
      (unless (pg-test-is-cratedb con)
        (when (fboundp 'bignump)
          (should (eql (scalar "SELECT factorial(25)") 15511210043330985984000000)))))
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
    (should (isnan (scalar "SELECT 'NaN'::float4")))
    (should (isnan (scalar "SELECT 'NaN'::float8")))
    (should (string= (scalar "SELECT 42::decimal::text") "42"))
    (should (string= (scalar "SELECT macaddr '08002b:010203'") "08:00:2b:01:02:03"))
    (should (eql (scalar "SELECT char_length('foo')") 3))
    (should (string= (scalar "SELECT lower('FOO')") "foo"))
    (should (eql (scalar "SELECT ascii('a')") 97))
    (should (eql (length (scalar "SELECT repeat('Q', 5000)")) 5000))
    (let ((4days (scalar "SELECT interval '1 day' + interval '3 days'")))
      (should (or (string= 4days "4 days")
                  ;; CrateDB prints the result in this way (valid if not hugely helpful)
                  (string= 4days "4 days 00:00:00"))))
    ;; CrateDB returns this as a string "3 days 00:00:00"
    (unless (pg-test-is-cratedb con)
      (should (eql (scalar "SELECT date '2001-10-01' - date '2001-09-28'") 3)))))

(defun pg-test-numeric-range (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
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
    (should (equal (list :range ?\( nil ?\) nil) (scalar "SELECT numrange(NULL, NULL)")))))


;; https://www.postgresql.org/docs/current/datatype-xml.html#DATATYPE-XML-CREATING
;;
;; We are handling XML as an Emacs Lisp string. PostgreSQL is not always compiled with
;; XML support, so check for that first.
(defun pg-test-xml (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (unless (or (pg-test-is-cockroachdb con)
                (pg-test-is-yugabyte con))
      (unless (zerop (scalar "SELECT COUNT(*) FROM pg_type WHERE typname='xml'"))
        (should (string= "<foo attr=\"45\">bar</foo>"
                         (scalar "SELECT XMLPARSE (CONTENT '<foo attr=\"45\">bar</foo>')")))
        (should (string= (scalar "SELECT xmlforest('abc' AS foo, 123 AS bar)")
                         "<foo>abc</foo><bar>123</bar>"))))))

;; https://www.postgresql.org/docs/current/datatype-uuid.html
(defun pg-test-uuid (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (scalar/p (sql args) (car (pg-result (pg-exec-prepared con sql args) :tuple 0))))
    (should (string= "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                     (scalar "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid")))
    (should (string= "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                     (scalar "SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid")))
    ;; Apparently only defined from PostgreSQL v13 onwards.
    (when (pg-function-p con "gen_random_uuid")
      (dotimes (_i 30)
        (let ((uuid (scalar "SELECT gen_random_uuid()"))
              (re (concat "\\<[[:xdigit:]]\\{8\\}-"
                          "[[:xdigit:]]\\{4\\}-"
                          "[[:xdigit:]]\\{4\\}-"
                          "[[:xdigit:]]\\{4\\}-"
                          "[[:xdigit:]]\\{12\\}\\>")))
          (should (string-match re uuid)))))
    (should
     (string=
      "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      (scalar/p "SELECT $1" `(("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" . "uuid")))))
    (should
     (string=
      ;; PostgreSQL returns the UUId in canonical (lowercase) format.
      "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      (scalar/p "SELECT $1" `(("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11" . "uuid")))))))


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
  (pg-exec con "CREATE TABLE byteatest(id INT PRIMARY KEY, blob BYTEA)")
  (pg-exec con "INSERT INTO byteatest VALUES(1, 'warning\\000'::bytea)")
  (pg-exec con "INSERT INTO byteatest VALUES(2, '\\001\\002\\003'::bytea)")
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
    (should (equal "warning " (scalar "SELECT blob FROM byteatest WHERE id=1")))
    (should (equal (string 1 2 3) (scalar "SELECT blob FROM byteatest WHERE id=2")))
    ;; When sending binary data to PostgreSQL, either encode all potentially problematic octets
    ;; like NUL (as above), or send base64-encoded content and decode in PostgreSQL.
    (let* ((size 512)
           (random-octets (make-string size 0)))
      (dotimes (i size)
        (setf (aref random-octets i) (random 256)))
      (setf (aref random-octets 0) 0)
      (pg-exec con (format "INSERT INTO byteatest VALUES (3, decode('%s', 'base64'))"
                            (base64-encode-string random-octets)))
      (should (equal random-octets (scalar "SELECT blob FROM byteatest WHERE id=3")))))
     (pg-exec con "DROP TABLE byteatest"))

(defun pg-test-sequence (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-exec con "DROP SEQUENCE IF EXISTS foo_seq")
    (pg-exec con "CREATE SEQUENCE IF NOT EXISTS foo_seq INCREMENT 20 START WITH 400")
    (should (equal 400 (scalar "SELECT nextval('foo_seq')")))
    (unless (pg-test-is-yugabyte con)
      (should (equal 400 (scalar "SELECT last_value FROM pg_sequences WHERE sequencename='foo_seq'"))))
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
      (should (floatp (aref vec 0)))
      (should (approx= 3.14 (aref vec 0))))
    (let ((vec (scalar "SELECT ARRAY[CAST(3.14 AS DOUBLE PRECISION)]")))
      (should (floatp (aref vec 0)))
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

(defun pg-test-metadata (con)
  ;; Check that the pg_user table exists and that we can parse the name type
  (let* ((res (pg-exec con "SELECT usename FROM pg_user"))
         (users (pg-result res :tuples)))
    (should (> (length users) 0)))
  (pg-exec con "DROP TABLE IF EXISTS coldefault")
  (pg-exec con "CREATE TABLE coldefault(id SERIAL PRIMARY KEY, comment TEXT)")
  ;; note that the id column has a DEFAULT value due to the SERIAL
  (pg-exec con "INSERT INTO coldefault(comment) VALUES ('foobles')")
  (should (pg-column-default con "coldefault" "id"))
  (should (pg-column-autogenerated-p con "coldefault" "id"))
  (should (not (pg-column-default con "coldefault" "comment")))
  (should (not (pg-column-autogenerated-p con "coldefault" "comment")))
  (pg-exec con "DROP TABLE coldefault")
  (pg-exec con "DROP TABLE IF EXISTS colgen_id")
  ;; GENERATED ALWAYS AS IDENTITY is now recommended instead of SERIAL
  ;; https://www.naiyerasif.com/post/2024/09/04/stop-using-serial-in-postgres/
  (pg-exec con "CREATE TABLE colgen_id(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, comment TEXT)")
  (pg-exec con "INSERT INTO colgen_id(comment) VALUES('bizzles')")
  ;; A generated column does not have a DEFAULT, in the PostgreSQL sense
  (should (not (pg-column-default con "colgen_id" "id")))
  (should (pg-column-autogenerated-p con "colgen_id" "id"))
  (should (not (pg-column-default con "colgen_id" "comment")))
  (should (not (pg-column-autogenerated-p con "colgen_id" "comment")))
  (pg-exec con "DROP TABLE colgen_id")
  (pg-exec con "DROP TABLE IF EXISTS colgen_expr")
  (pg-exec con "CREATE TABLE colgen_expr(count INTEGER, double INTEGER GENERATED ALWAYS AS (count*2) STORED)")
  (pg-exec con "INSERT INTO colgen_expr(count) VALUES(5)")
  (should (not (pg-column-default con "colgen_expr" "double")))
  (should (not (pg-column-default con "colgen_expr" "count")))
  (should (not (pg-column-autogenerated-p con "colgen_expr" "count")))
  (should (pg-column-autogenerated-p con "colgen_expr" "double"))
  (pg-exec con "DROP TABLE colgen_expr"))

;; TODO: we do not currently handle multidimension arrays correctly
;; (should (equal (vector (vector 4 5) (vector 6 7))
;;                (scalar "SELECT '{{4,5},{6,7}}'::int8[][]")))))

;; Schemas for qualified names such as public.tablename.
(defun pg-test-schemas (con)
  (let ((res (pg-exec con "CREATE SCHEMA IF NOT EXISTS custom")))
    (should (zerop (cl-search "CREATE" (pg-result res :status)))))
  (let ((res (pg-exec con "CREATE TABLE IF NOT EXISTS custom.newtable(id INT4 PRIMARY KEY)")))
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
  (let* ((sql (format "CREATE TABLE IF NOT EXISTS %s.%s(id INT4 PRIMARY KEY)"
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
  (let* ((sql (format "CREATE TABLE IF NOT EXISTS %s.%s(id INT4 PRIMARY KEY)"
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
         (sql (format "CREATE TABLE IF NOT EXISTS %s(id INT4 PRIMARY KEY)"
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
    ;; Note that Yugabyte for example has very large disk storage.
    (should (<= 0 size 10000000)))
  (let* ((qn (make-pg-qualified-name :schema "fan.cy" :name "tri\"cks"))
         (pqn (pg-print-qualified-name qn))
         (sql "SELECT pg_total_relation_size($1)")
         (res (pg-exec-prepared con sql `((,pqn . "text"))))
         (size (cl-first (pg-result res :tuple 0))))
    (should (<= 0 size 10000000)))
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


;; https://www.postgresql.org/docs/current/datatype-enum.html
;;
;; PostgreSQL support for ENUMs: defining a new ENUM leads to the creation of a new PostgreSQL OID
;; value for the new type. This means our cache mapping oid to type name, created when we establish
;; the connection, might become invalid and need to be refreshed.
(defun pg-test-enums (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-exec con "DROP TYPE IF EXISTS rating CASCADE")
    (pg-exec con "CREATE TYPE rating AS ENUM('ungood', 'good', 'plusgood',"
             "'doubleplusgood', 'plusungood', 'doubleplusungood')")
    (pg-exec con "DROP TABLE IF EXISTS act")
    (pg-exec con "CREATE TABLE act(name TEXT PRIMARY KEY, value RATING)")
    (pg-exec con "INSERT INTO act VALUES('thoughtcrime', 'doubleplusungood')")
    (pg-exec con "INSERT INTO act VALUES('thinkpol', 'doubleplusgood')")
    (pg-exec-prepared con "INSERT INTO act VALUES('blackwhite', $1)" `(("good" . "rating")))
    (message "Rating plusgood is %s" (scalar "SELECT 'plusgood'::rating"))
    (pg-exec con "DROP TABLE act")
    (pg-exec con "DROP TYPE rating")))


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
      (let* ((res (pg-exec con "SELECT jsonb_array_elements('[true,false,42]'::jsonb)"))
             (rows (pg-result res :tuples)))
        (should (equal '((t) (:false) (42)) rows)))
      (let ((json (scalar "SELECT json_object_agg(42, 66)")))
        (should (eql 66 (gethash "42" json))))
      (let ((json (scalar "SELECT '{\"a\":1,\"b\":-22}'::json")))
        (should (eql 1 (gethash "a" json)))
        (should (eql -22 (gethash "b" json))))
      (let ((json (scalar "SELECT '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json")))
        (should (string= "bar" (gethash "b" (aref json 1)))))
      (let ((json (scalar "SELECT '{\"a\": [0,1,2,null]}'::json")))
        (should (eql 2 (aref (gethash "a" json) 2)))))
    (should (string= "true" (scalar "SELECT 'true'::jsonpath")))
    (should (string= "$[*]?(@ < 1 || @ > 5)" (scalar "SELECT '$[*] ? (@ < 1 || @ > 5)'::jsonpath")))
    (let* ((sql "SELECT jsonb_path_query($1::jsonb, $2)")
           (res (pg-exec-prepared con sql `(("{\"h\": 9.2}" . "text") ("$.h.floor()" . "jsonpath"))))
           (row (pg-result res :tuple 0)))
      (should (eql 9 (cl-first row))))
    (let* ((sql "SELECT jsonb_path_query($1, $2)")
           (dict (make-hash-table :test #'equal))
           (_ (puthash "h" 5.6 dict))
           (params `((,dict . "jsonb") ("$.h.floor()" . "jsonpath")))
           (res (pg-exec-prepared con sql params))
           (row (pg-result res :tuple 0)))
      (should (eql 5 (cl-first row))))
    (when (>= (pgcon-server-version-major con) 17)
      ;; The json_scalar function is new in PostgreSQL 17.0, as is the .bigint() JSON path function
      (let* ((sql "SELECT jsonb_path_query(to_jsonb($1), $2)")
             (big 12567833445508910)
             (query "$.bigint()")
             (res (pg-exec-prepared con sql `((,big . "int8") (,query . "jsonpath"))))
             (row (pg-result res :tuple 0)))
        (should (eql big (cl-first row))))
      (let* ((sql "SELECT jsonb_path_query(cast(json_scalar($1) as jsonb), $2)")
             (tstamp "12:34:56.789 +05:30")
             (query "$.time_tz(2)")
             (res (pg-exec-prepared con sql `((,tstamp . "text") (,query . "jsonpath"))))
             (row (pg-result res :tuple 0)))
        (should (string= "12:34:56.79+05:30" (cl-first row))))
      ;; The json_array function is new in PostgreSQL 17.0
      (let* ((sql "SELECT json_array('pg-el', NULL, 42)")
	     (res (pg-exec con sql))
	     (row (pg-result res :tuple 0)))
	;; Default is to drop nulls in the input list
	(should (equal (vector "pg-el" 42) (cl-first row))))
      (let* ((sql "SELECT json_array('pg-el', NULL, 42 NULL ON NULL)")
	     (res (pg-exec con sql))
	     (row (pg-result res :tuple 0)))
	;; Default is to drop nulls in the input list
	(should (equal (vector "pg-el" :null 42) (cl-first row)))))))



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
          (puthash (format "föéê-%d" i) (format "bâçé-%d" i) hs)
          (puthash (format "a👿%d" (1+ i)) (format "%d" (- i)) hs)
          (pg-exec-prepared con "INSERT INTO hstored(meta) VALUES ($1)"
                            `((,hs . "hstore")))))
      (let ((rows (scalar "SELECT COUNT(*) FROM hstored")))
        (should (eql 10 rows)))
      (let* ((res (pg-exec con "SELECT meta FROM hstored"))
             (rows (pg-result res :tuples)))
        (dolist (ht (mapcar #'car rows))
          (maphash (lambda (k v)
                     (should (or (cl-search "foobles" k)
                                 (cl-search "föéê" k)
                                 (eql ?a (aref k 0))
                                 (cl-search "a👿" k)))
                     (should (or (cl-search "bazzles" v)
                                 (cl-search "bâçé" v)
                                 (ignore-errors (string-to-number v)))))
                   ht))))))


;; Testing support for the pgvector extension.
(defun pg-test-vector (con)
  (when (pg-vector-setup con)
    (message "Testing pgvector extension")
    (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
      (let ((v (scalar "SELECT '[4,5,6]'::vector")))
        (should (eql 4 (aref v 0))))
      (let ((v (scalar "SELECT '[0.003,0.004,1.567,6.777]'::vector")))
        (should (eql 4 (length v)))
        (should (<= 6 (aref v 3) 7)))
      (let ((d (scalar "SELECT inner_product('[1,2]'::vector, '[3,4]')")))
        (should (eql 11 d)))
      (let ((d (scalar "SELECT l2_distance('[0,0]'::vector, '[3,4]')")))
        (should (eql 5 d)))
      (let ((d (scalar "SELECT cosine_distance('[1,2]'::vector, '[0,0]')")))
        (should (eql 0.0e+NaN d)))
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

(defun pg-test-geometric (con)
  (cl-labels ((row (query args) (pg-result (pg-exec-prepared con query args) :tuple 0))
              (scalar (query args) (car (row query args)))
              (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (pg-geometry-setup con)
    (let* ((raw "(45.6,67) ")
           (p1 (pg--point-parser "(45.6,67) " nil)))
      (message "Parse of point %s -> %s" raw p1)
      (should (eql 67 (cdr p1))))
    (let ((p2 (pg--point-parser "  (33, 0)" nil)))
      (should (eql 33 (car p2))))
    (let ((p3 (pg--point-parser " (0.34, -9.111111145677888)    " nil)))
      (should (floatp (cdr p3)))
      (should (<= -10 (cdr p3) -9)))
    (let ((p4 (pg--point-parser "(33e4, -3.1456e4)" nil)))
      (should (approx= 33e4 (car p4)))
      (should (<= -35000 (cdr p4) 31000)))
    (let ((p5 (pg--point-parser "(a,b)" nil)))
      (should (eql nil p5)))
    (should (eql nil (pg--point-parser "" nil)))
    (let ((p7 (pg--point-parser "(3,4)" nil)))
      (should (eql 3 (car p7)))
      (should (eql 4 (cdr p7))))
    (let ((p8 (pg--point-parser "(12.1,4e-4) " nil)))
      (should (approx= 4 (* 1e4 (cdr p8)))))
    (let ((p9 (pg--point-parser " (55,7866677)" nil)))
      (should (eql 55 (car p9))))
    (let ((p10 (pg--point-parser "(22.6,6) " nil)))
      (should (eql 6 (cdr p10))))
    (let ((point (scalar "SELECT '(82,91.0)'::point" nil)))
      (should (consp point))
      (should (eql 82 (car point)))
      (should (approx= 91.0 (cdr point))))
    (pg-exec con "DROP TABLE IF EXISTS with_point")
    (pg-exec con "CREATE TABLE with_point(id SERIAL PRIMARY KEY, p POINT)")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(33,44)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(33.1,4.4)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(1,0)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(3.12345663,78.1)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(-4,44)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(3,-55.7)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(1,111e-5)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES('(34e3,1e10)')")
    (pg-exec con "INSERT INTO with_point(p) VALUES(NULL)")
    (pg-exec-prepared con "INSERT INTO with_point(p) VALUES($1)"
                      `((,(cons 45.5 0.111) . "point")))
    (let* ((p1 (cons 2 3))
           (res (pg-exec-prepared con "SELECT $1" `((,p1 . "point"))))
           (row (pg-result res :tuple 0))
           (point (cl-first row)))
      (should (eql 2 (car point))))
    (let* ((res (pg-exec con "SELECT * FROM with_point"))
           (rows (pg-result res :tuples)))
      (dolist (row rows)
        (message "Point out --> %s" (cl-second row))))
    (pg-exec con "DROP TABLE with_point")
    (let ((l1 (pg--line-parser "{45.6,1.11,-2.9}" nil)))
      (should (<= 45 (aref l1 0) 46)))
    (pg-exec con "DROP TABLE IF EXISTS with_line")
    (pg-exec con "CREATE TABLE with_line(id SERIAL PRIMARY KEY, ln LINE)")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{1,2,3}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{-1,2,-3}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{1.55,-0.234,3e6}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{0, 34.9999992,-3e2}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES(' {7,-44.44, 2.1}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{ 0,01, 03.00}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{-3.567e-4,2,3}')")
    (pg-exec con "INSERT INTO with_line(ln) VALUES('{1,0.0000,3e-3}')")
    (let* ((ln (vector -5 -6 -7))
           (res (pg-exec-prepared con "SELECT $1" `((,ln . "line"))))
           (row (pg-result res :tuple 0))
           (line (cl-first row)))
      (should (eql -5 (aref line 0)))
      (should (eql -6 (aref line 1)))
      (should (eql -7 (aref line 2))))
    (let* ((res (pg-exec con "SELECT * FROM with_line"))
           (rows (pg-result res :tuples)))
      (dolist (row rows)
        (message "Line out --> %s" (cl-second row))))
    (pg-exec con "DROP TABLE with_line")
    (let ((lseg (pg--lseg-parser " [(4,5), (6.7, 4e1)]" nil)))
      (should (eql 4 (car (aref lseg 0))))
      (should (approx= 4e1 (cdr (aref lseg 1)))))
    (pg-exec con "DROP TABLE IF EXISTS with_lseg")
    (pg-exec con "CREATE TABLE with_lseg(id SERIAL PRIMARY KEY, ls LSEG)")
    (pg-exec con "INSERT INTO with_lseg(ls) VALUES('[(4,5), (6.6,7.7)]')")
    (let* ((ls (vector (cons 2 3) (cons 55.5 66.6)))
           (res (pg-exec-prepared con "SELECT $1" `((,ls . "lseg"))))
           (ls (cl-first (pg-result res :tuple 0))))
      (should (eql 2 (car (aref ls 0))))
      (should (eql 3 (cdr (aref ls 0))))
      (should (approx= 55.5 (car (aref ls 1)))))
    (pg-exec con "DROP TABLE with_lseg")
    (let ((box (pg--box-parser "(4,5), (-66,-77e0) " nil)))
      (should (eql 4 (car (aref box 0))))
      (should (eql -66 (car (aref box 1)))))
    (pg-exec con "DROP TABLE IF EXISTS with_box")
    (pg-exec con "CREATE TABLE with_box(id SERIAL PRIMARY KEY, bx BOX)")
    (pg-exec con "INSERT INTO with_box(bx) VALUES('(33.3,5),(5,-67e1)')")
    (let* ((bx (vector (cons 2 3) (cons 55.6 -23.2)))
           (res (pg-exec-prepared con "SELECT $1" `((,bx . "box"))))
           (bx (cl-first (pg-result res :tuple 0))))
      ;; the box corners are output in the order upper-right, lower-left
      (should (approx= 55.6 (car (aref bx 0))))
      (should (eql 3 (cdr (aref bx 0))))
      (should (approx= -23.2 (cdr (aref bx 1)))))
    (pg-exec con "DROP TABLE with_box")
    (let* ((path (pg--path-parser "[(4,5),(6,7), (55e1,66.1),(0,0) ]" nil))
           (points (pg-geometry-path-points path)))
      (should (eql :open (pg-geometry-path-type path)))
      (should (eql 4 (length points)))
      (should (eql 7 (cdr (nth 1 points)))))
    (pg-exec con "DROP TABLE IF EXISTS with_path")
    (pg-exec con "CREATE TABLE with_path(id SERIAL PRIMARY KEY, pt PATH)")
    (pg-exec con "INSERT INTO with_path(pt) VALUES('[(22,33.3), (4.5,1)]')")
    (pg-exec con "INSERT INTO with_path(pt) VALUES('[(22,33.3), (4.5,1),(-66,-1)]')")
    (pg-exec con "INSERT INTO with_path(pt) VALUES('((22,33.3),(4.5,1),(0,0),(0,0))')")
    (let* ((pth (make-pg-geometry-path :type :closed
                                       :points '((2 . 3) (4 . 5) (55.5 . 66.6) (-1 . -1))))
           (res (pg-exec-prepared con "SELECT $1" `((,pth . "path"))))
           (pth (cl-first (pg-result res :tuple 0)))
           (points (pg-geometry-path-points pth)))
      (should (eql 4 (length points)))
      (should (eql :closed (pg-geometry-path-type pth)))
      (should (eql 3 (cdr (cl-first points))))
      (should (eql -1 (car (cl-fourth points)))))
    (pg-exec con "DROP TABLE with_path")
    (let* ((polygon (pg--polygon-parser "((4,5), (6,7),(55.0,-43.0),(1,1),(0,0))" nil))
           (points (pg-geometry-polygon-points polygon)))
      (should (eql 5 (length points)))
      (should (eql 4 (car (cl-first points))))
      (should (eql 5 (cdr (cl-first points))))
      (should (eql 0 (car (car (last points))))))
    (pg-exec con "DROP TABLE IF EXISTS with_polygon")
    (pg-exec con "CREATE TABLE with_polygon(id SERIAL PRIMARY KEY, pg POLYGON)")
    (pg-exec con "INSERT INTO with_polygon(pg) VALUES('((3,4),(5,6),(44.4,55.5))')")
    (let* ((pg (make-pg-geometry-polygon :points '((2 . 3) (3 . 4) (4 . 5) (6.6 . 7.77))))
           (res (pg-exec-prepared con "SELECT $1" `((,pg . "polygon"))))
           (pg (cl-first (pg-result res :tuple 0)))
           (points (pg-geometry-polygon-points pg)))
      (should (eql 4 (length points)))
      (should (eql 3 (cdr (cl-first points))))
      (should (approx= 7.77 (cdr (car (last points))))))
    (pg-exec con "DROP TABLE with_polygon")))

;; PostGIS parsing tests. These tests require the geosop commandline utility to be installed.
(defun pg-test-gis (con)
  (cl-labels ((row (query) (pg-result (pg-exec con query) :tuple 0))
              (scalar (query) (car (row query)))
              (approx= (x y) (< (/ (abs (- x y)) (max (abs x) (abs y))) 1e-5)))
    (when (pg-setup-postgis con)
      (message "Testing PostGIS support...")
      (let* ((res (pg-exec con "SELECT 'SRID=4326;POINT(0 0)'::geometry"))
             (tuple (pg-result res :tuple 0)))
        (message "GIS/POINT> %s" (car tuple)))
      (should (string= (scalar "SELECT 'POINT(4 5)'::geometry") "POINT (4 5)"))
      (let ((pg-gis-use-geosop nil))
        (should (string= (scalar "SELECT 'POINT(4 5)'::geometry")
                         "010100000000000000000010400000000000001440")))
      (should (string= (scalar "SELECT 'SRID=4326;POINT(45 70.0)'::geometry") "POINT (45 70)"))
      (should (string= (scalar "SELECT 'MULTILINESTRING((-118.584 38.374 20,-118.583 38.5 30),(-71.05957 42.3589 75, -71.061 43 90))'::geometry")
                       "MULTILINESTRING Z ((-118.584 38.374 20, -118.583 38.5 30), (-71.05957 42.3589 75, -71.061 43 90))"))
      (should (string= (scalar "SELECT 'GEOMETRYCOLLECTION(POINT(2 0),POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))'::geometry")
                       "GEOMETRYCOLLECTION (POINT (2 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))"))
      (should (string= (scalar "SELECT 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'::geometry")
                       "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"))
      (should (string= (scalar "SELECT 'POINT(2 43)'::geography") "POINT (2 43)"))
      (should (string= (scalar "SELECT 'POINT(2.223 43.001)'::geography") "POINT (2.223 43.001)"))
      (should (string= (scalar "SELECT ST_GeographyFromText('POINT(2.5559 49.0083)')")
                       "POINT (2.5559 49.0083)"))
      (should (string= (scalar "SELECT 'SRID=4326;POINT(45 80.0)'::geography")
                       ;; "0101000020E610000000000000008046400000000000005440"
                       "POINT (45 80)"))
      (should (string= (scalar "SELECT 'SPHEROID[\"GRS_1980\",6378137,298.2572]'::spheroid")
                       "SPHEROID(\"GRS_1980\",6378137,298.2572)"))
      (should (string= (scalar "SELECT Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))")
                       "BOX(1 2,5 6)"))
      (should (string= (scalar "SELECT ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')::box2d")
                       "BOX(1 2,5 6)"))
      (should (string= (scalar "SELECT Box3D(ST_GeomFromEWKT('LINESTRING(1 2 3, 3 4 5, 5 6 5)'))")
                       "BOX3D(1 2 3,5 6 5)"))
      (should (string= (scalar "SELECT Box3D(ST_GeomFromEWKT('CIRCULARSTRING(220268 150415 1,220227 150505 1,220227 150406 1)'))")
                       "BOX3D(220186.99512189245 150406 1,220288.24878054656 150506.12682932706 1)")))))

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
  (let ((r1 (pg-exec con "CREATE TABLE resulttest (a INT PRIMARY KEY, b VARCHAR(4))"))
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
   (unless (pg-test-is-xata con)
     (let ((res (pg-exec con "EXPLAIN ANALYZE SELECT 42")))
       (should (string= "EXPLAIN" (pg-result res :status)))
       (should (cl-every (lambda (r) (stringp (car r))) (pg-result res :tuples)))))
   ;; check query with empty column list
   (let ((res (pg-exec con "SELECT from information_schema.routines")))
     (should (eql nil (pg-result res :attributes)))
     (should (cl-every #'null (pg-result res :tuples)))))


(defun pg-test-cursors (con)
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
    (should (string= "COMMIT" (pg-result res :status)))))


(defun pg-test-createdb (con)
  (when (member "pgeltestextra" (pg-databases con))
    (pg-exec con "DROP DATABASE pgeltestextra"))
  (pg-exec con "CREATE DATABASE pgeltestextra")
  (should (member "pgeltestextra" (pg-databases con)))
  ;; CockroachDB and YugabyteDB don't implement REINDEX. Also, REINDEX at the database level is
  ;; disabled on certain installations (e.g. Supabase), so we check reindexing of a table.
  (unless (or (pg-test-is-cockroachdb con)
              (pg-test-is-yugabyte con))
    (pg-exec con "DROP TABLE IF EXISTS foobles")
    (pg-exec con "CREATE TABLE foobles(a INTEGER PRIMARY KEY, b TEXT)")
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
  (pg-exec-prepared con "CREATE SCHEMA IF NOT EXISTS un␂icode" nil)
  (pg-exec-prepared con "CREATE TABLE IF NOT EXISTS un␂icode.ma🪄c(data TEXT PRIMARY KEY)" nil)
  (pg-exec-prepared con "INSERT INTO un␂icode.ma🪄c VALUES($1)" '(("hi" . "text")))
  (let ((r (pg-exec con "SELECT * FROM un␂icode.ma🪄c")))
    (should (eql 1 (length (pg-result r :tuples)))))
  (pg-exec con "DROP TABLE un␂icode.ma🪄c")
  (pg-exec con "DROP SCHEMA un␂icode")
  (pg-exec con "CREATE TEMPORARY TABLE pgeltestunicode(pg→el TEXT PRIMARY KEY)")
  (pg-exec con "INSERT INTO pgeltestunicode(pg→el) VALUES ('Foobles')")
  (pg-exec con "INSERT INTO pgeltestunicode(pg→el) VALUES ('Bizzles')")
  (let ((r (pg-exec con "SELECT pg→el FROM pgeltestunicode")))
    (should (eql 2 (length (pg-result r :tuples)))))
  ;; Check that Emacs is doing Unicode normalization for us. The first 'á' is LATIN SMALL LETTER A
  ;; with COMBINING ACUTE ACCENT, the second 'á' is the normalized form LATIN SMALL LETTER A WITH
  ;; ACUTE. If you run this query in psql the answer will be false, because psql does not do Unicode
  ;; normalization. With pg-el, the query is encoded to the client-encoding UTF-8 using function
  ;; encode-coding-string, but this encoding does not involve normalization.
  (let ((r (pg-exec con "SELECT 'á' = 'á'")))
    (should (eql nil (cl-first (pg-result r :tuple 0)))))
  (let ((r (pg-exec-prepared con "SELECT $1 = $2" '(("á" . "text") ("á" . "text")))))
    (should (eql nil (cl-first (pg-result r :tuple 0))))))

(defun pg-test-returning (con)
  (when (member "pgeltestr" (pg-tables con))
    (pg-exec con "DROP TABLE pgeltestr"))
  (pg-exec con "CREATE TABLE pgeltestr(id SERIAL PRIMARY KEY, data TEXT)")
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
    ;; channel name is determined by a variable. It is not implemented in all
    ;; PostgreSQL-semi-compatible databases.
    (unless (pg-test-is-xata con)
      (pg-exec con "SELECT pg_notify('yourheart', 'leaving')"))
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

(defun pg-run-tz-tests (con)
  (pg-exec con "DROP TABLE IF EXISTS tz_test")
  (pg-exec con "CREATE TABLE tz_test(id INTEGER PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ)")
  (with-environment-variables (("TZ" "Europe/Berlin"))
    (pg-exec con "SET TimeZone = 'Europe/Berlin'")
    (unwind-protect
        (progn
          (pg-test-iso8601-regexp)
          (pg-test-parse-ts con)
          (pg-test-serialize-ts con)
          (pg-test-insert-literal-ts con)
          (pg-test-insert-parsed-ts con))
      (pg-exec con "DROP TABLE tz_test"))))

(defun pg-test-iso8601-regexp ()
  (message "Test iso8601 regexp ...")
  (let ((regexp pg--ISODATE_REGEX))
    (pg-assert-does-not-match "" regexp)
    (pg-assert-does-not-match "2024-02-2711:34:42.789+04" regexp)
    (pg-assert-does-not-match "2024-02-27T11:34:42+4" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901+04:00" regexp)
    (pg-assert-matches "2024-02-27 11:34:42.78901+04:00" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901" regexp)
    (pg-assert-matches "2024-02-27T11:34:42+04" regexp)
    (pg-assert-matches "2024-02-27T11:34:42" regexp)
    (pg-assert-matches "2024-02-27 11:34:42" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901+04:30" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901+04" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901+0430" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901Z" regexp)
    (pg-assert-matches "2024-02-27T11:34:42.78901z" regexp)))

(defun pg-test-parse-ts (con)
  (message "Test parsing of timestamps ...")
  (let ((ts (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789+04" nil))
        (ts-dst (pg-isodate-without-timezone-parser "2024-05-27T11:34:42.789+04" nil))
        (ts-no-tz (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789" nil))
        (ts-zulu (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789Z" nil))
        (tstz (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789+04" nil))
        (tstz-dst (pg-isodate-with-timezone-parser "2024-05-27T15:34:42.789+04" nil))
        (tstz-no-tz (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789" nil))
        (tstz-zulu (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789Z" nil)))
    ;; Without DST, there is a one hour difference between UTC and Europe/Berlin.
    (pg-assert-equals "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts))
    ;; With DST (switchover is in March), there is a two hour difference between UTC and Europe/Berlin.
    (pg-assert-equals "2024-05-27T09:34:42.789+0000" (pg-fmt-ts-utc ts-dst))
    (pg-assert-equals "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts-no-tz))
    (pg-assert-equals "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts-zulu))
    (pg-assert-equals "2024-02-27T11:34:42.789+0000" (pg-fmt-ts-utc tstz))
    (pg-assert-equals "2024-05-27T11:34:42.789+0000" (pg-fmt-ts-utc tstz-dst))
    (pg-assert-equals "2024-02-27T14:34:42.789+0000" (pg-fmt-ts-utc tstz-no-tz))
    (pg-assert-equals "2024-02-27T15:34:42.789+0000" (pg-fmt-ts-utc tstz-zulu))))

(defun pg-test-serialize-ts (con)
  (message "Test serialization of timestamps ...")
  (let* ((ts (encode-time (iso8601-parse "2024-02-27T15:34:42.789+04" t)))
         (ts-ser (pg--serialize-encoded-time ts nil)))
    (pg-assert-equals "2024-02-27T11:34:42.789000000+0000" ts-ser)))

(defun pg-test-insert-literal-ts (con)
  (message "Test literal (string) timestamp insertion ...")
  ;; We take this as reference. It behaves exactly like psql.
  ;; Entering literals works as expected. Note that we cast to text to rule out deserialization errors.
  (pg-exec con "SET TimeZone = 'Etc/UTC'")
  (pg-exec con "INSERT INTO tz_test(id, ts, tstz) VALUES(1, '2024-02-27T11:34:42.789+04', '2024-02-27T15:34:42.789+04')")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=1") :tuple 0))
         (ts (nth 0 data))
         (tstz (nth 1 data)))
    (pg-assert-equals "2024-02-27 11:34:42.789" ts)
    (pg-assert-equals "2024-02-27 11:34:42.789+00" tstz))
  (pg-exec con "SET TimeZone = 'Europe/Berlin'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=1") :tuple 0))
         (tstz (nth 1 data)))
    (pg-assert-equals "2024-02-27 12:34:42.789+01" tstz)))

(defun pg-test-insert-parsed-ts (con)
  (message "Test object timestamp insertion ...")
  (pg-exec-prepared con "INSERT INTO tz_test(id, ts, tstz) VALUES(2, $1, $2)"
                    `((,(pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789+04" nil) . "timestamp")
                      (,(pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789+04:00" nil) . "timestamptz")))
  (pg-exec con "SET TimeZone = 'Etc/UTC'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=2") :tuple 0))
         (ts (nth 0 data))
         (tstz (nth 1 data)))
    (pg-assert-equals "2024-02-27 10:34:42.789" ts)
    (pg-assert-equals "2024-02-27 11:34:42.789+00" tstz))
  (pg-exec con "SET TimeZone = 'Europe/Berlin'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=1") :tuple 0))
         (tstz (nth 1 data)))
    (pg-assert-equals "2024-02-27 12:34:42.789+01" tstz)))


(defun pg-assert-equals (expected actual)
  (should (string= expected actual)))

(defun pg-assert-matches (str regexp)
  (should (string-match regexp str)))

(defun pg-assert-does-not-match (str regexp)
  (should-not (string-match regexp str)))

(defun pg-fmt-ts-utc (ts)
  (let ((ft "%Y-%m-%dT%H:%M:%S.%3N%z"))
    (format-time-string ft ts "UTC")))


;; EOF
