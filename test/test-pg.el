;;; Tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; Copyright: (C) 2022-2025  Eric Marsden
;; SPDX-License-Identifier: GPL-3.0-or-later


(require 'cl-lib)
(require 'hex-util)
(require 'pg)
(require 'pg-geometry)
(require 'pg-gis)
(require 'pg-bm25)
(require 'pg-lo)
(require 'ert)


(defvar pgtest--enable-query-log nil)
(setq debug-on-error t)


;; for performance testing
(setq process-adaptive-read-buffering t)


;; https://www.reidatcheson.com/floating%20point/comparison/2019/03/20/floating-point-comparison.html
(defun pgtest-approx= (x y)
  (let ((smallest (min x y)))
    (if (= (abs smallest) 0.0)
        (< (abs (- x y)) 1e-5)
      (< (/ (abs (- x y)) (max 1e-8 smallest)) 1e-5))))


;; Good practice for PostgreSQL is to replace use of the SERIAL type by "GENERATED ALWAYS AS
;; IDENTITY". However, several of the PostgreSQL variants that we want to test don't implement this
;; syntax, so we choose the syntax for this when we establish a connection.
;;
;; https://www.naiyerasif.com/post/2024/09/04/stop-using-serial-in-postgres/
;;
;; Documentation on the way SERIAL is implemented in CockroachDB:
;;   https://www.cockroachlabs.com/docs/stable/serial.html
;;
;; This formats SQL where %s is replaced by the appropriate SERIAL/AUTOINCREMENT type, or returns
;; NIL if this variant does not support an autoincrementing integer type.
(cl-defun pgtest-massage (con sql &rest fmt-args)
  (let ((serial (pcase (pgcon-server-variant con)
                  ('postgresql
                   (if (< (pgcon-server-version-major con) 12)
                       "SERIAL"
                     "BIGINT GENERATED ALWAYS AS IDENTITY"))
                  ('cratedb "TEXT DEFAULT gen_random_text_uuid()")
                  ;; https://www.cockroachlabs.com/docs/stable/serial.html#generated-values-for-mode-sql_sequence
                  ('cockroachdb "UUID NOT NULL DEFAULT gen_random_uuid()")
                  ;; RisingWave does not (2025-03) implement NOT NULL constraints, nor an autoincrementing column type.
                  ('risingwave nil)
                  ('questdb "UUID NOT NULL DEFAULT gen_random_uuid()")
                  ('materialize nil)
                  ('yellowbrick nil)
                  (_ "SERIAL")))
        (pk (pcase (pgcon-server-variant con)
              ('materialize "")
              ('questdb "")
              (_ "PRIMARY KEY"))))
    (when (cl-search "SERIAL" sql)
      (if serial
          (setq sql (string-replace "SERIAL" serial sql))
        ;; If serial is nil, this variant doesn't implement an equivalent to SERIAL
        (cl-return-from pgtest-massage nil)))
    (setq sql (string-replace "PRIMARY KEY" pk sql))
    (apply #'format (cons sql fmt-args))))


;; Some PostgreSQL variants that focus on high-performance distributed operation operate with
;; "eventually consistent" semantics, and require an explict sync-like operation to ensure that
;; inserted rows are visible to a SELECT query.
(defun pgtest-flush-table (con table)
  (pcase (pgcon-server-variant con)
    ('cratedb
     (pg-exec con (format "REFRESH TABLE %s" table)))
    ('risingwave
     (pg-exec con "FLUSH"))))

(cl-defun pgtest-have-table (con table)
  (let* ((cs (pg-current-schema con))
         (qtable (if (pg-qualified-name-p table)
                     table
                   (make-pg-qualified-name :schema cs :name table))))
    (cl-flet ((matches (target)
                (let ((qtarget (if (pg-qualified-name-p target)
                                   target
                                 (make-pg-qualified-name :schema cs :name target))))
                  (equal qtarget qtable))))
      (dolist (tbl (pg-tables con))
        (when (matches tbl)
          (cl-return-from pgtest-have-table t)))
      nil)))


(defmacro with-pgtest-connection (con &rest body)
  (declare (indent defun))
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
           `(with-pg-connection-plist ,con (,db ,user
                                            :password ,password
                                            :host ,host
                                            :port ,port
                                            :server-variant ',server-variant)
                 ,@body)))))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (con &rest body)
  (declare (indent defun))
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
           `(with-pg-connection-plist ,con (,db ,user
                                            :password ,password
                                            :host ,host
                                            :port ,port
                                            :tls-options t)
               ,@body)))))

;; Connect to the database using the "direct TLS" method introduced in PostgreSQL 18
(defmacro with-pgtest-connection-direct-tls (con &rest body)
  (declare (indent 1) (debug (symbolp body)))
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
                (trust-ca (getenv "PGEL_TRUST_CA"))
                (trust-ca-file (and trust-ca (expand-file-name trust-ca)))
                (trust (list :trustfiles (list trust-ca-file))))
           `(progn
              (unless ',trust-ca
                (error "Need PGEL_TRUST_CA env variable"))
              (let ((,con (pg-connect-plist ,db ,user
                                            :password ,password
                                            :host ,host
                                            :port ,port
                                            :direct-tls t
                                            :tls-options ',trust)))
                (unwind-protect
                    (progn ,@body)
                  (when ,con (pg-disconnect ,con)))))))))


;; Connect to the database presenting a client certificate as authentication
(defmacro with-pgtest-connection-client-cert (con &rest body)
  (declare (indent 1) (debug (symbolp body)))
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
             (with-pg-connection-plist ,con (,db ,user
                                             :password ,password
                                             :host ,host
                                             :port ,port
                                             :tls-options '(:keylist ((,key ,cert))))
                  ,@body))))))


(defmacro with-pgtest-connection-local (con &rest body)
  (declare (indent 1) (debug (symbolp body)))
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


;; These tests are written assuming that PostgreSQL is running on the standard port on localhost,
;; with our pgeltestuser account set up.
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
      (pg-disconnect con)))
  ;; Test that the application_name parameter is properly sent to PostgreSQL.
  (let* ((uri "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb?application_name=testingtesting")
         (con (pg-connect/uri uri)))
    (should (process-live-p (pgcon-process con)))
    (let* ((res (pg-exec con "SELECT current_setting('application_name')"))
           (row (pg-result res :tuple 0)))
      (should (string= (cl-first row) "testingtesting")))
    (pg-disconnect con))
  ;; Test the handling of the PGOPTIONS environment variable, and options URI/connection string param
  (with-environment-variables (("PGOPTIONS" "--client_min_messages=DEBUG4"))
    (let* ((con (pg-connect/uri "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb"))
           (res (pg-exec con "SELECT current_setting('client_min_messages')"))
           (row (pg-result res :tuple 0)))
      (should (string-equal-ignore-case (cl-first row) "DEBUG4"))
      (pg-disconnect con)))
  (with-environment-variables (("PGOPTIONS" "-c client_min_messages=DEBUG4"))
    (let* ((con (pg-connect/uri "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb"))
           (res (pg-exec con "SELECT current_setting('client_min_messages')"))
           (row (pg-result res :tuple 0)))
      (should (string-equal-ignore-case (cl-first row) "DEBUG4"))
      (pg-disconnect con)))
  (let* ((con (pg-connect/uri "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb?options=--client_min_messages%3DDEBUG3"))
         (res (pg-exec con "SELECT current_setting('client_min_messages')"))
         (row (pg-result res :tuple 0)))
    (should (string-equal-ignore-case (cl-first row) "DEBUG3"))
    (pg-disconnect con))
  (with-environment-variables (("PGOPTIONS" "-c search_path=foobles,fabbles -c client_min_messages=DEBUG4"))
    (let* ((con (pg-connect/uri "postgresql://pgeltestuser:pgeltest@localhost/pgeltestdb"))
           (res (pg-exec con "SELECT current_setting('client_min_messages'), current_setting('search_path')"))
           (row (pg-result res :tuple 0)))
      (should (string-equal-ignore-case (cl-first row) "DEBUG4"))
      (should (string= "foobles,fabbles" (cl-second row)))
      (pg-disconnect con)))
  (should (eql 'ok
               (let ((con nil))
                 (unwind-protect
                     (condition-case nil
                         (progn
                           (setq con (pg-connect-plist "nonexistent-db" "pgeltestuser" :password "pgeltest"))
                           (pg-exec con "SELECT 42"))
                       (pg-invalid-catalog-name 'ok))
                   (when con
                     (pg-disconnect con)))))))

;; Try very hard to reset the PostgreSQL connection, by sending a Sync message and multiple queries
;; that should hopefully flush any pending ErrorMessage messages in the pipeline. We call this
;; inbetween functions testing different functionality classes, hoping to prevent propagation of an
;; error between functions.
(defun pgtest-reset (con)
  (pg-sync con)
  (pg-exec con "SELECT 42")
  (pg-exec con "SELECT 'foobles'")
  (pg-exec con "SELECT 42")
  (pg-exec con "SELECT 42"))


(defun pg-run-tests (con)
  (let ((tests (list)))
    (cl-flet ((pgtest-add (fun &key skip-variants need-emacs)
                (unless (member (pgcon-server-variant con) skip-variants)
                  (when (if need-emacs (version<= need-emacs emacs-version) t)
                    (push fun tests)))))
      (when pgtest--enable-query-log
        (pg-enable-query-log con))
      (message "Backend major-version is %s" (pgcon-server-version-major con))
      (message "Detected backend variant: %s" (pgcon-server-variant con))
      (unless (member (pgcon-server-variant con)
                      '(cockroachdb cratedb yugabyte ydb xata greptimedb risingwave clickhouse octodb vertica arcadedb
                                    cedardb pgsqlite datafusion stoolap))
        (when (> (pgcon-server-version-major con) 11)
          (let* ((res (pg-exec con "SELECT current_setting('ssl_library')"))
                 (row (pg-result res :tuple 0)))
            (message "Backend compiled with SSL library %s" (cl-first row)))))
      (unless (member (pgcon-server-variant con)
                      '(questdb cratedb ydb xata greptimedb risingwave clickhouse materialize vertica arcadedb datafusion stoolap immudb))
        (let* ((res (pg-exec con "SHOW ssl"))
               (row (pg-result res :tuple 0)))
          (message "PostgreSQL connection TLS: %s" (cl-first row))))
      (message "Current schema: %s" (pg-current-schema con))
      (message "List of schemas in db: %s" (pg-schemas con))
      (message "List of tables in db: %s" (pg-tables con))
      ;; Special testing for pg_duckdb
      (when (cl-find "duckdb" (pg-schemas con) :test #'string=)
        (message "Activating duckdb.force_execution")
        (pg-exec con "SET duckdb.force_execution = true"))
      (when (eq 'orioledb (pgcon-server-variant con))
        (pg-exec con "CREATE EXTENSION IF NOT EXISTS orioledb"))
      (unless (member (pgcon-server-variant con) '(clickhouse alloydb risingwave stoolap pgsqlite))
        (pg-setup-postgis con))
      (unless (member (pgcon-server-variant con) '(clickhouse risingwave stoolap arcadedb pgsqlite))
        (pg-vector-setup con))
      (pgtest-add #'pg-test-basic)
      (pgtest-add #'pg-test-insert)
      (pgtest-add #'pg-test-edge-cases)
      (pgtest-add #'pg-test-procedures
                  :skip-variants '(cratedb spanner risingwave materialize ydb xata questdb thenile vertica greptimedb datafusion))
      ;; RisingWave is not able to parse a TZ value of "UTC-01:00" (POSIX format). QuestDB does not
      ;; support the timestamptz type. CedarDB des not support the timetz data type.
      (pgtest-add #'pg-test-date
                  :skip-variants '(cratedb risingwave materialize ydb questdb cedardb clickhouse)
                  :need-emacs "29.1")
      ;; QuestDB does not support the timestamptz column type.
      (pgtest-add #'pg-run-tz-tests
                  :skip-variants '(risingwave materialize ydb clickhouse spanner questdb readyset))
      ;; Vertica does not implement types like int2
      (pgtest-add #'pg-test-numeric
                  :skip-variants '(vertica))
      (pgtest-add #'pg-test-numeric-range
                  :skip-variants '(xata cratedb cockroachdb ydb risingwave questdb clickhouse greptimedb spanner octodb vertica cedardb datafusion immudb stoolap))
      (pgtest-add #'pg-test-prepared
                  :skip-variants '(ydb cratedb)
                  :need-emacs "28")
      ;; Risingwave v2.2.0 panics on this test
      ;; (https://github.com/risingwavelabs/risingwave/issues/20367). Vertica does not implement
      ;; generate_series()
      (pgtest-add #'pg-test-prepared/multifetch
                  :skip-variants '(risingwave ydb vertica)
                  :need-emacs "28")
      (pgtest-add #'pg-test-insert/prepared
                  :skip-variants '(ydb)
                  :need-emacs "28")
      ;; Risingwave v2.2.0 raises a spurious error "Duplicated portal name" here
      (pgtest-add #'pg-test-ensure-prepared
                  :skip-variants '(risingwave ydb)
                  :need-emacs "28")
      (pgtest-add #'pg-test-collation
                  :skip-variants '(xata cratedb questdb clickhouse greptimedb octodb vertica yellowbrick datafusion immudb))
      (pgtest-add #'pg-test-xml
                  :skip-variants '(xata ydb cockroachdb yugabyte clickhouse alloydb vertica opengauss datafusion))
      (pgtest-add #'pg-test-uuid
                  :skip-variants '(cratedb risingwave ydb clickhouse greptimedb spanner octodb vertica yellowbrick datafusion))
      ;; Risingwave doesn't support VARCHAR(N) type. YDB and Vertica don't support SELECT generate_series().
      (pgtest-add #'pg-test-result
                  :skip-variants  '(risingwave ydb spanner clickhouse vertica))
      (pgtest-add #'pg-test-cursors
                  :skip-variants '(xata cratedb cockroachdb risingwave questdb greptimedb ydb materialize spanner octodb
                                        cedardb yellowbrick datafusion))
      ;; CrateDB does not support the BYTEA type (!), nor sequences. Spanner does not support the encode() function.
      (pgtest-add #'pg-test-bytea
                  :skip-variants '(cratedb risingwave spanner materialize))
      ;; Spanner does not support the INCREMENT clause in CREATE SEQUENCE. Vertica does not
      ;; implement the pg_sequences system table.
      (pgtest-add #'pg-test-sequence
                  :skip-variants '(cratedb risingwave questdb materialize greptimedb ydb spanner clickhouse thenile
                                           vertica yellowbrick opengauss datafusion immudb))
      (pgtest-add #'pg-test-array
                  :skip-variants '(cratedb risingwave questdb materialize clickhouse octodb))
      (pgtest-add #'pg-test-enums
                  :skip-variants '(cratedb risingwave questdb greptimedb ydb materialize spanner octodb clickhouse
                                           vertica cedardb yellowbrick datafusion immudb))
      (pgtest-add #'pg-test-server-prepare
                  :skip-variants '(cratedb risingwave questdb greptimedb ydb octodb datafusion))
      (pgtest-add #'pg-test-comments
                   :skip-variants '(ydb cratedb cockroachdb spanner questdb thenile cedardb datafusion))
      (pgtest-add #'pg-test-metadata
                  :skip-variants '(cratedb cockroachdb risingwave materialize questdb greptimedb ydb spanner vertica datafusion))
      ;; CrateDB doesn't support the JSONB type. CockroachDB doesn't support casting to JSON.
      (pgtest-add #'pg-test-json
                  :skip-variants '(xata cratedb risingwave questdb greptimedb ydb materialize spanner octodb vertica cedardb datafusion immudb))
      (pgtest-add #'pg-test-schemas
                  :skip-variants '(xata cratedb risingwave questdb ydb materialize yellowbrick))
      (pgtest-add #'pg-test-hstore
                  :skip-variants '(risingwave materialize octodb readyset vertica cockroachdb datafusion))
      ;; Xata doesn't support extensions, but doesn't signal an SQL error when we attempt to load the
      ;; pgvector extension, so our test fails despite being intended to be robust.
      (pgtest-add #'pg-test-vector
                  :skip-variants '(xata cratedb materialize octodb vertica))
      (pgtest-add #'pg-test-tsvector
                  :skip-variants '(xata cratedb cockroachdb risingwave questdb greptimedb ydb materialize spanner
                                        octodb vertica cedardb yellowbrick datafusion))
      (pgtest-add #'pg-test-bm25
                  :skip-variants '(xata cratedb cockroachdb risingwave materialize octodb vertica))
      (pgtest-add #'pg-test-geometric
                  :skip-variants '(xata cratedb cockroachdb risingwave questdb materialize spanner octodb vertica cedardb
                                        yellowbrick datafusion))
      (pgtest-add #'pg-test-gis
                  :skip-variants '(xata cratedb cockroachdb risingwave materialize octodb datafusion))
      (pgtest-add #'pg-test-copy
                  :skip-variants '(spanner ydb cratedb risingwave materialize questdb xata vertica yellowbrick datafusion))
      ;; QuestDB fails due to lack of support for the NUMERIC type
      (pgtest-add #'pg-test-copy-large
                  :skip-variants '(spanner ydb cratedb risingwave questdb materialize datafusion))
      ;; Apparently Xata does not support CREATE DATABASE
      (pgtest-add #'pg-test-createdb
                  :skip-variants '(xata cratedb questdb ydb vertica immudb))
      ;; Many PostgreSQL variants only support UTF8 as the client encoding.
      (pgtest-add #'pg-test-client-encoding
                  :skip-variants '(cratedb cockroachdb ydb risingwave materialize spanner greptimedb questdb xata vertica datafusion))
      (pgtest-add #'pg-test-unicode-names
                  :skip-variants '(xata cratedb cockroachdb risingwave questdb ydb spanner vertica immudb))
      (pgtest-add #'pg-test-returning
                  :skip-variants '(risingwave questdb datafusion immudb))
      (pgtest-add #'pg-test-parameter-change-handlers
                  :skip-variants '(cratedb risingwave))
      (pgtest-add #'pg-test-errors)
      ;; CrateDB and Risingwave signal all errors as SQLSTATE XX000 meaning "internal error", rather
      ;; than returning a more granular error code.
      (pgtest-add #'pg-test-error-sqlstate
                  :skip-variants '(cratedb risingwave))
      ;; As of 2025-08, CedarDB does not implement DO.
      (pgtest-add #'pg-test-notice
                  :skip-variants '(cedardb datafusion))
      (pgtest-add #'pg-test-notify
                  :skip-variants '(cratedb cockroachdb risingwave materialize greptimedb ydb questdb spanner vertica cedardb
                                           yellowbrick opengauss datafusion))
      (pgtest-add #'pg-test-lo
                  :skip-variants '(cratedb cockroachdb risingwave materialize greptimedb ydb questdb spanner vertica greenplum
                                           cedardb yellowbrick opengauss datafusion))
      (dolist (test (reverse tests))
        (message "== Running test %s" test)
        (condition-case err
            (funcall test con)
          (error (message "\033[31;1mTest failed\033[0m: %s" err)))
        (pgtest-reset con))
      (message "== Tests finished; producing a report on memory usage")
      (memory-report)
      (with-current-buffer "*Memory Report*"
        (message "%s" (buffer-string))))))


(defun pg-test-note-param-change (_con name value)
  (message "PG> backend parameter %s=%s" name value))

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

;; For the "direct TLS" connection method introduced in PostgreSQL v18.
(defun pg-test-tls-direct ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection-direct-tls con
       (message "Running pg.el tests in %s against backend %s"
                (version) (pg-backend-version con))
       (pg-run-tests con))))

;; Run tests over local Unix socket connection to backend
(defun pg-test-local ()
  (let ((pg-parameter-change-functions (cons #'pg-test-note-param-change pg-parameter-change-functions)))
    (with-pgtest-connection-local con
       (message "Running pg.el tests in %s against backend %s"
                (version) (pg-backend-version con))
       (pg-run-tests con))))

;; Simple connect and list tables test on a public RNAcentral PostgreSQL server hosted at ebi.ac.uk, see
;;  https://rnacentral.org/help/public-database.
(defun pg-test-ebiacuk ()
  (let ((con (pg-connect/uri "postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk/pfmegrnargs?read_timeout=1000")))
    (message "Connected to %s, %s"
             (cl-prin1-to-string con)
             (pg-backend-version con))
    (dolist (table (pg-tables con))
      (message "  Table: %s" table))
    ;; Here test that a very long resultset is received correctly on this Emacs platform.
    (cl-labels ((row (sql) (pg-result (pg-exec con sql) :tuple 0))
                (scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
      (let ((long (scalar "SELECT repeat('z', 1000000) || 'foo'")))
        (should (eql 1000003 (length long)))
        (should (string-prefix-p "zzz" long))
        (should (string-suffix-p "foo" long))))))


(defun pg-test-prepared (con)
  (cl-labels ((row (query args) (pg-result (pg-exec-prepared con query args) :tuple 0))
              (scalar (query args) (car (row query args))))
    (should (equal 42 (scalar "SELECT 42" (list))))
    (should (pgtest-approx= 42.0 (scalar "SELECT 42.00" (list))))
    (should (equal pg-null-marker (scalar "SELECT NULL" (list))))
    (unless (member (pgcon-server-variant con) '(immudb))
      (should (equal (list t nil) (row "SELECT $1, $2" `((t . "bool") (nil . "bool")))))
      (should (equal (list -33 "ZZz" 9999) (row "SELECT $1,$2,$3" `((-33 . "int4") ("ZZz" . "text") (9999 . "int8"))))))
    (unless (member (pgcon-server-variant con) '(risingwave))
      (pg-exec con "DEALLOCATE ALL"))
    (unless (member (pgcon-server-variant con) '(ydb))
      (should (equal nil (scalar "" (list)))))
    (unless (member (pgcon-server-variant con) '(risingwave))
      (pg-exec con "PREPARE pgtest_foobles(integer) AS SELECT $1 + 1")
      (let* ((res (pg-exec con "EXECUTE pgtest_foobles(41)"))
             (row (pg-result res :tuple 0)))
        (should (eql 42 (cl-first row)))))
    ;; https://github.com/kagis/pgwire/blob/main/test/test.js
    (let ((typ (scalar "SELECT pg_typeof($1)::text" '((42 . "int4")))))
      (should (or (string= "integer" typ)
                  (string= "int4" typ)
                  (string= "bigint" typ))))
    (let ((typs (row "SELECT pg_typeof($1)::text, $1::text" '(("foobles" . "text")))))
      (should (string= "foobles" (cl-second typs)))
      (should (or (string= "text" (cl-first typs))
                  ;; RisingWave returns this
                  (string= "character varying" (cl-first typs)))))
    (unless (member (pgcon-server-variant con) '(cratedb risingwave materialize ydb yellowbrick datafusion))
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
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "smallint")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "integer")))))
    (should (eql 42 (scalar "SELECT $1 + 142" '((-100 . "bigint")))))
    (should (eql 100 (scalar "SELECT $1" '((100 . "oid")))))
    (should (pgtest-approx= -55.0 (scalar "SELECT $1" '((-55.0 . "float4")))))
    (should (pgtest-approx= -55.0 (scalar "SELECT $1" '((-55.0 . "float8")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float4")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 85.0" '((-43.0 . "float4")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 85.0" '((-43.0 . "numeric")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 1" '((41.0 . "float8")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 85" '((-43.0 . "float8")))))
    (should (pgtest-approx= 42.0 (scalar "SELECT $1 + 85" '((-43.0 . "numeric")))))
    (unless (member (pgcon-server-variant con) '(cratedb risingwave))
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
    (unless (or (member (pgcon-server-variant con) '(cratedb))
                (zerop (scalar "SELECT COUNT(*) FROM pg_collation WHERE collname='fr-FR'" nil)))
      (should (string= "12 foé£èüñ¡" (scalar "SELECT lower($1) COLLATE \"fr-FR\"" '(("12 FOÉ£ÈÜÑ¡" . "text"))))))
    ;; Risingwave failed to parse the PT12S
    (unless (member (pgcon-server-variant con) '(risingwave materialize))
      (should (equal "00:00:12" (scalar "SELECT $1::interval" '(("PT12S" . "text"))))))
    (should (equal -1 (scalar "SELECT $1::int" '((-1 . "int4")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float4" '((1.0e+INF . "float4")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float8" '((1.0e+INF . "float8")))))
    (should (eql 1.0e+INF (scalar "SELECT $1::float8" '((1.0e+INF . "numeric")))))
    (should (eql 0.0e+NaN (scalar "SELECT $1::float4" '((0.0e+NaN . "float4")))))
    (should (eql 0.0e+NaN (scalar "SELECT $1::float8" '((0.0e+NaN . "float8")))))
    (unless (member (pgcon-server-variant con) '(cedardb))
      (should (eql 0.0e+NaN (scalar "SELECT $1::numeric" '((0.0e+NaN . "numeric"))))))
    ;; CrateDB does not support the BYTEA type.
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (equal (byte-to-string 0)
                     (scalar "SELECT $1::bytea" '(("\\000" . "text")))))
      (should (equal (byte-to-string 0)
                     (scalar "SELECT $1" `((,(byte-to-string 0) . "bytea")))))
      (should (equal (decode-hex-string "DEADBEEF")
                     (scalar "SELECT $1::bytea" '(("\\xDEADBEEF" . "text")))))
      (should (equal (decode-hex-string "DEADBEEF")
                     (scalar "SELECT $1" `((,(decode-hex-string "DEADBEEF") . "bytea"))))))
    ;; Risingwave does not support casting to JSON.
    (unless (member (pgcon-server-variant con) '(risingwave materialize))
      (let ((json (scalar "SELECT $1::json" '(("[66.7,-42.0,8]" . "text")))))
        (should (pgtest-approx= 66.7 (aref json 0)))
        (should (pgtest-approx= -42.0 (aref json 1)))))
    ;; CrateDB does not support the JSONB type, nor casting {foo=bar} syntax to JSON. CockroachDB
    ;; supports JSONB but not JSON.
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb risingwave materialize))
      (let ((json (scalar "SELECT $1::jsonb" '(("[66.7,-42.0,8]" . "text")))))
        (should (pgtest-approx= 66.7 (aref json 0)))
        (should (pgtest-approx= -42.0 (aref json 1))))
      (let ((json (scalar "SELECT $1::jsonb" '(("[5,7]" . "text")))))
        (should (eql 5 (aref json 0))))
      (let* ((ht (make-hash-table :test #'equal))
             (_ (puthash "say" "foobles" ht))
             (_ (puthash "biz" 42 ht))
             (json (scalar "SELECT $1::json" `((,ht . "json")))))
        (should (equal "foobles" (gethash "say" json)))
        (should (equal 42 (gethash "biz" json)))))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb risingwave materialize))
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
          (should (string= (gethash "comment" (cl-first row)) "good stuff")))))
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
           (_ (pgtest-flush-table con "students"))
           (count (cl-first (row "SELECT COUNT(*) FROM students" nil)))
           (name (cl-first (row "SELECT name FROM students LIMIT 1" nil))))
      (should (string-prefix-p "INSERT" (pg-result res :status)))
      (should (eql 1 count))
      (should (cl-search "Robert" name)))
    (should-error (scalar "SELECT * FROM" '(("a" . "text"))))
    (pg-sync con)
    (should-error (scalar "SELECT $1::int4" '(("2147483649" . "int4"))))
    (pg-sync con)
    ;; Test error handling
    (should (eql 'ok (condition-case nil
                         (pg-exec-prepared con "SELECT $1 + 1" '(("XXXX" . "int4")))
                       (pg-type-error 'ok))))
    (should (eql 'ok (condition-case nil
                         (pg-exec-prepared con "SELECT $1" '(("XXXX" . "date")))
                       (pg-type-error 'ok))))
    (should (eql 'ok (condition-case nil
                         (pg-exec-prepared con "SELECT $1 * $2" '(("2" . "int4") ("4" . "int8") ("foobles" . "text")))
                       (pg-type-error 'ok))))))
    


;; Materialize is returning incorrect values here, failing the test.
(cl-defun pg-test-prepared/multifetch (con &optional (rows 1000))
  (let* ((res (pg-exec-prepared con "" nil))
         (tuples (pg-result res :tuples)))
    (should (eql 0 (length tuples)))
    (should (not (pg-result res :incomplete))))
  (let* ((res (pg-exec-prepared con "SELECT generate_series(1, $1)"
                                `((,rows . "int4"))
                                :max-rows 10))
         (portal (pgresult-portal res))
         (counter 0))
    (should (pg-result res :incomplete))
    ;; check the results from the initial pg-exec-prepared
    (dolist (tuple (pg-result res :tuples))
      (should (eql (cl-first tuple) (cl-incf counter))))
    ;; keep fetching and checking more rows until the portal is complete
    (while (pg-result res :incomplete)
      (setq res (pg-fetch con res :max-rows 7))
      (dolist (tuple (pg-result res :tuples))
        (should (eql (cl-first tuple) (cl-incf counter)))))
    (should (eql counter rows))
    (pg-close-portal con portal))
  ;; check for unexpected pending messages in the stream (problem with old PostgreSQL versions)
  (let* ((res (pg-exec con "SELECT 55"))
         (tuple (pg-result res :tuple 0)))
    (should (eql 55 (cl-first tuple)))))


;; https://github.com/postgres/postgres/blob/master/src/test/regress/sql/insert.sql
;;
;; https://github.com/denodrivers/postgres/blob/main/tests/data_types_test.ts
(defun pg-test-basic (con)
  (cl-labels ((row (sql) (pg-result (pg-exec con sql) :tuple 0))
              (scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (should (equal (list 42) (row "SELECT 42")))
    (should (equal (list t) (row "SELECT true")))
    (unless (member (pgcon-server-variant con) '(immudb))
      (should (equal (list t nil) (row "SELECT true, false")))
      (should (equal (list -33 "ZZ" 9999) (row "SELECT -33, 'ZZ', 9999"))))
    (should (eql -1 (scalar "SELECT -1::integer")))
    (should (eql 66 (scalar "SELECT 66::int2")))
    (should (eql -66 (scalar "SELECT -66::int2")))
    (should (eql 44 (scalar "SELECT 44::int4")))
    (should (eql -44 (scalar "SELECT -44::int4")))
    (should (eql 12345 (scalar "SELECT 12345::int8")))
    (should (eql -12345 (scalar "SELECT -12345::int8")))
    (should (eql 100 (scalar "SELECT CAST ('100' AS INTEGER)")))
    (should (eql nil (scalar "SELECT '0'::boolean")))
    (should (eql t (scalar "SELECT '1'::boolean")))
    (should (eql -6 (scalar "SELECT -(6)")))
    (should (eql ?Z (scalar "SELECT 'Z'::char")))
    (should (eql ?@ (scalar "SELECT '@'::char(1)")))
    (should (eql 97 (scalar "SELECT ascii('a')")))
    (should (eql 0 (scalar "SELECT ascii('')")))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb cedardb spanner))
      (should (eql ?! (scalar "SELECT '!'::bpchar(1)"))))
    (should (string= "Z" (scalar "SELECT 'Z'::varchar")))
    (should (string= "É" (scalar "SELECT 'É'::varchar(1)")))
    (should (string= "AB" (scalar "SELECT 'AB'::char(2)")))
    (should (string= "ÁÔ" (scalar "SELECT 'ÁÔ'::char(2)")))
    (should (string= "ÁÔ" (scalar "SELECT 'ÁÔ'::varchar(2)")))
    (should (string= "3" (scalar "SELECT CAST (1+2 AS text)")))
    (should (string= "3" (scalar "SELECT CAST (1+2 AS varchar)")))
    (should (string= "3" (scalar "SELECT CAST (1+2 AS varchar(10))")))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb cedardb spanner))
      (should (string= "12" (scalar "SELECT '12'::bpchar(2)"))))
    (should (string= "£Öí" (scalar "SELECT '£Öí'::text")))
    (should (string= "Albert" (scalar "SELECT 'Albert'::name")))
    (should (string= "AB" (scalar "SELECT 'AB'::varchar(4)")))
    ;; The string is stored internally with space padding. Note that PostgreSQL will automatically
    ;; strip the space padding upon server-side conversion to TEXT or VARCHAR; for example SELECT
    ;; '{' || 'A'::character(10) || '}' only returns a TEXT string of length 3, rather than of
    ;; length 12.
    (should (string= "AB   " (scalar "SELECT 'AB'::character(5)")))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb cedardb spanner))
      (should (string= "AB    " (scalar "SELECT 'AB'::bpchar(6)"))))
    ;; Just to note that the space padding is stripped before "semantic comparison", as per
    ;; https://www.postgresql.org/docs/current/datatype-character.html
    (should (eql t (scalar "SELECT 'A'::char(1000) = 'A'::char(10)")))
    (should (eql t (scalar "SELECT true or false")))
    (should (equal (list "hey" "Jude") (row "SELECT 'hey', 'Jude'")))
    (should (eql pg-null-marker (scalar "SELECT NULL")))
    (unless (member (pgcon-server-variant con) '(cratedb risingwave yugabyte xata))
      (when (> (pgcon-server-version-major con) 15)
        (should (eql #x1eeeffff (scalar "SELECT int8 '0x1EEE_FFFF'")))))
    (should (eql t (scalar "SELECT 42 = 42")))
    (should (eql nil (scalar "SELECT 53 = 33")))
    (should (eql 42 (scalar "SELECT /* FREE PALESTINE */ 42 ")))
    (should (equal (list 1 pg-null-marker "all") (row "SELECT 1,NULL,'all'")))
    (unless (member (pgcon-server-variant con) '(questdb spanner))
      (should (string= "Z" (scalar "SELECT chr(90)"))))
    (should (eql 12 (scalar "SELECT length('(╯°□°)╯︵ ┻━┻')")))
    (should (eql 37 (scalar "SELECT length('Text Line إلا بسم الله 🥝 𒐫  a⃰⃰⃰⃰⃰⃰⃰ ')")))
    (should (string= "::!!::" (scalar "SELECT '::!!::'::varchar")))
    (should (string= "éàç⟶∪" (scalar "SELECT 'éàç⟶∪'")))
    ;; Note that we need to escape the ?\ character in an elisp string by repeating it.
    ;; CrateDB does not support the BYTEA type.
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (eql 3 (length (scalar "SELECT '\\x123456'::bytea"))))
      (should (string= (string #x12 #x34 #x56) (scalar "SELECT '\\x123456'::bytea"))))
    (unless (member (pgcon-server-variant con) '(spanner))
      (should (eql nil (row " SELECT 3 WHERE 1=0"))))
    (should (eql 4 (scalar "SELECT ((2 * 2))")))
    (should (string= "abcdef" (scalar "SELECT 'abc' || 'def'")))
    (should (equal pg-null-marker (scalar "SELECT NULL || NULL")))
    (should (string= "abc" (scalar "SELECT concat('abc', NULL)")))
    (should (string= "foo69" (scalar "SELECT concat('foo', 69)")))
    (should (string= "howdy" (scalar "SELECT 'howdy'::text")))
    (should (eql t (scalar "SELECT 'abc' LIKE 'a%'")))
    (should (string= "banana" (scalar "SELECT split_part('apple,banana,cherry', ',', 2)")))
    ;; RisingWave does not support the VARCHAR(N) syntax.
    (unless (eq 'risingwave (pgcon-server-variant con))
      (should (string= "gday" (scalar "SELECT 'gday'::varchar(20)"))))
    (should (equal pg-null-marker (scalar "SELECT SUM(null::numeric) FROM generate_series(1,3)")))
    ;; CrateDB: Cannot cast `'NaN'` of type `text` to type `numeric`
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (eql 0.0e+NaN (scalar "SELECT SUM('NaN'::numeric) FROM generate_series(1,3)"))))
    ;; CockroachDB is returning these byteas in a non-BYTEA format so they are twice as long as
    ;; expected. CrateDB does not implement the sha256 and sha512 functions.
    ;;
    ;; Could use digest('foobles', 'sha1') if we loaded the pgcrypto extension.
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb))
      (should (eql 32 (length (scalar "SELECT sha256('foobles')"))))
      (should (eql 64 (length (scalar "SELECT sha512('foobles')")))))
    ;; The MD5 function is not implemented by the Spanner variant. Note that it is also disabled in
    ;; some PostgreSQL builds which compile OpenSSL in a FIPS-compatible mode, but in that case the
    ;; function triggers a runtime error (and it doesn't seem to be possible to check at runtime
    ;; whether the function is correctly implemented or not).
    (when (pg-function-p con "md5")
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
    (unless (member (pgcon-server-variant con) '(ydb))
      (let* ((version-str (car (row "SELECT current_setting('server_version_num')")))
             (version-num (and version-str (cl-parse-integer version-str))))
        (if version-str
            (should (eql (pgcon-server-version-major con)
                         (/ version-num 10000)))
          (message "This PostgreSQL server doesn't support current_setting('server_version_num')"))))))


(defun pg-test-edge-cases (con)
  (cl-labels ((row (sql) (pg-result (pg-exec con sql) :tuple 0))
              (scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    ;; YDB hangs on this empty query.
    (unless (member (pgcon-server-variant con) '(ydb))
      (let ((res (pg-exec con "")))
        (should (string-equal-ignore-case (pg-result res :status) "EMPTY"))))
    (unless (member (pgcon-server-variant con) '(cratedb clickhouse))
      (should (eql t (scalar "SELECT bool 'f' < bool 't' AS true")))
      (should (eql t (scalar "SELECT bool 'f' <= bool 't' AS true"))))
    ;; Empty strings are equal
    (should (eql t (scalar "SELECT '' = ''")))
    (should (eql 0 (scalar "SELECT length('')")))
    (should (eql pg-null-marker (scalar "SELECT length(null)")))
    (when (pg-function-p con "bit_length")
      (should (eql pg-null-marker (scalar "SELECT bit_length(null)"))))
    (should (eql 0 (scalar "SELECT length(lower(''))")))
    (should (eql 0 (scalar "SELECT length('' || '')")))
    (should (string= "" (scalar "SELECT substring('foobles' from 2 for 0)")))
    (should (eql 0 (scalar "SELECT length(concat('', NULL, ''))")))
    (should (string= "" (scalar "SELECT ''::VARCHAR")))
    (should (string= "" (scalar "SELECT substr('foobles', 8, 0)")))
    (should (equal pg-null-marker (scalar "SELECT substring('foo' FROM null::text)")))
    ;; Returns NULL because NULL is weird in SQL
    (should (equal pg-null-marker (scalar "SELECT NULL = NULL")))
    ;; IS checks for NULL identity
    (should (eql t (scalar "SELECT NULL IS NULL")))
    (should (eql nil (scalar "SELECT NULL IS DISTINCT FROM NULL")))
    (should (equal pg-null-marker (scalar "SELECT NULL::integer")))
    ;; NULL is propagated in mathematical operations.
    (should (equal pg-null-marker (scalar "SELECT NULL + 42")))
    (should (equal pg-null-marker (scalar "SELECT 42 < NULL")))
    (should (equal pg-null-marker (scalar "SELECT ABS(NULL)")))
    ;; NULL is propagated in logical operations.
    (should (equal pg-null-marker (scalar "SELECT true AND NULL")))
    (should (equal pg-null-marker (scalar "SELECT true <= NULL")))
    (should (equal pg-null-marker (scalar "SELECT NULL < NULL")))
    (should (equal pg-null-marker (scalar "SELECT NOT NULL")))
    (should (equal pg-null-marker (scalar "SELECT NULL BETWEEN 1 and 42")))
    (should (equal pg-null-marker (scalar "SELECT 42 BETWEEN 1 and NULL")))
    (should (equal pg-null-marker (scalar "SELECT 42 & NULL")))
    (should (equal pg-null-marker (scalar "SELECT 42 | NULL")))
    (should (equal pg-null-marker (scalar "SELECT coalesce(NULL)")))
    (should (eql t (scalar "SELECT TRUE OR NULL")))
    ;; This leads to a timeout with YDB
    (unless (member (pgcon-server-variant con) '(ydb))
      (should (equal nil (row ""))))
    (unless (member (pgcon-server-variant con) '(cratedb risingwave))
      (should (eql nil (scalar "SELECT"))))
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (eql nil (row "-- comment")))
      (should (eql nil (row "  /* only a comment */ "))))
    (should (eql 42 (scalar "SELECT 42; -- comment")))
    (should (eql 42 (scalar "SELECT /* Free Palestine */ 42 -- more ")))
    (should (eql 42 (scalar "SELECT 40
-- more
-- that
-- is
-- ignored
+ 2")))
    ;; This statement is strangely very poorly supported in semi-compatible PostgreSQL variants...
    (unless (member (pgcon-server-variant con) '(cratedb risingwave))
      (let* ((res (pg-exec con "SELECT 42 as éléphant"))
             (col1 (cl-first (pg-result res :attributes))))
        (should (string= "éléphant" (cl-first col1)))))
    (let* ((res (pg-exec con "SELECT -55 AS \"foo/bar\""))
           (col1 (cl-first (pg-result res :attributes)))
           (row (pg-result res :tuple 0)))
      (should (eql -55 (cl-first row)))
      (should (string= "foo/bar" (cl-first col1))))
    ;; Try a query with a large number of columns.
    (let* ((n 1200)
           (cols (cl-loop for i from 1 to n collect (format "%d AS col%d" (- i) i)))
           (sql (concat "SELECT " (string-join cols ", ")))
           (res (pg-exec con sql))
           (row (pg-result res :tuple 0))
           (columns (pg-result res :attributes)))
      (should (eql n (length row)))
      (should (eql -55 (elt row (1- 55))))
      (should (string= "col66" (cl-first (elt columns (1- 66)))))
      (should (string= "col555" (cl-first (elt columns (1- 555))))))
    (unless (member (pgcon-server-variant con) '(cratedb spanner ydb))
      ;; these are row expressions, not standard SQL
      (should (string= (scalar "SELECT (1,2)") "(1,2)"))
      (should (string= (scalar "SELECT (null,1,2)") "(,1,2)")))
    (unless (eq 'risingwave (pgcon-server-variant con))
      (should (string= "foo\nbar" (scalar "SELECT $$foo
bar$$"))))
    (should (string= "foo\tbar" (scalar "SELECT 'foo\tbar'")))
    (should (string= "foo\rbar\nbiz" (scalar "SELECT 'foo\rbar\nbiz'")))
    (should (eql -55 (scalar "SELECT \n\n-\n\n55  \t\n")))
    (should (equal (list 4 2 0) (row "SELECT 4,\n2,\n0")))
    (let* ((big (concat "SELECT length('foobles" (make-string (* 10 1024) ?0) "baz')"))
           (res (scalar big)))
      (should (numberp res)))
    (dolist (size (list 50 5000 500000))
      (let* ((sql (format "SELECT repeat('#', %d)" size))
             (str (scalar sql)))
        (should (eql size (length str)))))
    (let ((long (scalar "SELECT repeat('z', 1000000) || 'foo'")))
      (should (eql 1000003 (length long)))
      (should (string-prefix-p "zzz" long))
      (should (string-suffix-p "foo" long)))))


(defun pg-test-insert (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0)))
            (random-word () (apply #'string (cl-loop for count to 15 collect (+ ?a (random 26))))))
    (let ((count 100))
      (when (pgtest-have-table con "count_test")
        (pg-exec con "DROP TABLE count_test"))
      (let ((sql (pgtest-massage con "CREATE TABLE count_test(mykey INT PRIMARY KEY, val INT) %s"
                                 (if (eq 'orioledb (pgcon-server-variant con))
                                     " USING orioledb"
                                   ""))))
        (pg-exec con sql))
      (should (pgtest-have-table con "count_test"))
      (should (member "val" (pg-columns con "count_test")))
      (unless (member (pgcon-server-variant con) '(cratedb xata ydb spanner questdb thenile))
        (let ((user (or (nth 4 (pgcon-connect-info con))
                        "pgeltestuser"))
              (owner (pg-table-owner con "count_test")))
          ;; Some hosted PostgreSQL servers that require you to use a username of the form
          ;; myuser@the-hostname only return "myuser" from pg-table-owner.
          (unless (cl-search "@" user)
            (should (string= user owner))
            (should (string= user (pg-table-owner con (make-pg-qualified-name :name "count_test"))))
            (should (string= user (pg-table-owner con "count_test"))))))
      (cl-loop for i from 1 to count
               for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                                 i (* i i))
               do (pg-exec con sql))
      (unless (member (pgcon-server-variant con) '(cratedb cockroachdb ydb risingwave materialize xata thenile))
        (pg-exec con "VACUUM ANALYZE count_test"))
      (pgtest-flush-table con "count_test")
      (should (eql count (scalar "SELECT count(*) FROM count_test")))
      (should (eql (/ (* count (1+ count)) 2) (scalar "SELECT sum(mykey) FROM count_test")))
      (pg-exec con "DROP TABLE count_test")
      (should (not (pgtest-have-table con "count_test"))))
    ;; Test for specific bugs when we have a table name and column names of length 1 (could be
    ;; interpreted as a character rather than as a string).
    (pg-exec con "DROP TABLE IF EXISTS w")
    (when-let* ((sql (pgtest-massage con "CREATE TABLE w(i SERIAL PRIMARY KEY, v TEXT)")))
      (pg-exec con sql)
      (unless (member (pgcon-server-variant con) '(ydb))
        (setf (pg-table-comment con "w") "c"))
      (should (stringp (pg-table-owner con "w")))
      (pg-exec con "INSERT INTO w(v) VALUES ('s')")
      (pg-exec con "INSERT INTO w(v) VALUES ('é')")
      (pg-exec-prepared con "INSERT INTO w(v) VALUES($1)" `(("a" . "text")))
      (pgtest-flush-table con "w")
      (let ((res (pg-exec con "SELECT * FROM w")))
        (should (eql 3 (length (pg-result res :tuples)))))
      (pg-exec con "DROP TABLE w"))
    ;; Testing insert via UNNEST. YDB does not support unnest on _text,_float. 
    (unless (member (pgcon-server-variant con) '(ydb))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE measurement(
         id SERIAL PRIMARY KEY,
         sensorid TEXT,
         value FLOAT8,
         ts TIMESTAMP DEFAULT current_timestamp)")))
        (pg-exec con "DROP TABLE IF EXISTS measurement")
        (pg-exec con sql)
        (let* ((size 39)
               (sensors (make-vector size nil))
               (values (make-vector size 0.0))
               (sql "INSERT INTO measurement(sensorid,value) SELECT * FROM unnest($1::text[], $2::float8[])"))
          (dotimes (i size)
            (setf (aref sensors i) (random-word))
            (setf (aref values i) (cl-random 1000.0)))
          (pg-exec-prepared con sql
                            `((,sensors . "_text") (,values . "_float8")))
          (pgtest-flush-table con "measurement")
          (let* ((res (pg-exec con "SELECT COUNT(*) FROM measurement"))
                 (row (pg-result res :tuple 0)))
            (should (eql size (cl-first row))))
          (pg-exec con "DROP TABLE measurement"))))))

(defun pg-test-insert/prepared (con)
  (cl-flet ((scalar (sql) (cl-first (pg-result (pg-exec con sql) :tuple 0))))
    (let ((count 100))
      (when (pgtest-have-table con "count_test")
        (pg-exec con "DROP TABLE count_test"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE count_test(mykey INT PRIMARY KEY, val INT)")))
        (pg-exec con sql)
        (should (pgtest-have-table con "count_test"))
        (should (member "val" (pg-columns con "count_test")))
        (unless (member (pgcon-server-variant con) '(cratedb risingwave ydb materialize))
          (pg-exec con "TRUNCATE TABLE count_test"))
        (dotimes (i count)
          (pg-exec-prepared con "INSERT INTO count_test VALUES($1, $2)"
                            `((,i . "int4") (,(* i i) . "int4"))))
        (pgtest-flush-table con "count_test")
        (should (eql count (scalar "SELECT COUNT(*) FROM count_test")))
        (should (eql (/ (* (1- count) count) 2) (scalar "SELECT sum(mykey) FROM count_test")))
        (pg-exec con "DROP TABLE count_test"))
      (should (not (pgtest-have-table con "count_test")))
      ;; Serialization functions for character and varchar types
      (when (pgtest-have-table con "chararray")
        (pg-exec con "DROP TABLE chararray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE chararray(id SERIAL PRIMARY KEY, val CHAR)")))
        (pg-exec con sql)
        (pg-exec-prepared con "INSERT INTO chararray(val) VALUES($1)" '((?A . "char")))
        (pgtest-flush-table con "chararray")
        (should (eql ?A (scalar "SELECT val FROM chararray")))
        (pg-exec con "DROP TABLE chararray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE chararray(id SERIAL PRIMARY KEY, val CHAR(10))")))
        (pg-exec con sql)
        (pg-exec-prepared con "INSERT INTO chararray(val) VALUES($1)" '(("AAAA" . "text")))
        (pgtest-flush-table con "chararray")
        (should (string= "AAAA      " (scalar "SELECT val FROM chararray")))
        (pg-exec con "DROP TABLE chararray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE chararray(id SERIAL PRIMARY KEY, val VARCHAR(10))")))
        (pg-exec con sql)
        (pg-exec-prepared con "INSERT INTO chararray(val) VALUES($1)" '(("éééé" . "varchar")))
        (pgtest-flush-table con "chararray")
        (should (string= "éééé" (scalar "SELECT val FROM chararray")))
        (pg-exec con "DROP TABLE chararray"))
      ;; Now test the serialization functions for array types
      (when (pgtest-have-table con "sarray")
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val int2)")))
        (pg-exec con sql)
        (let* ((size 203)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::int2[])"))
          (dotimes (i size)
            (setf (aref values i) i))
          (pg-exec-prepared con sql `((,values . "_int2")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (eql i (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val int4)")))
        (pg-exec con sql)
        (let* ((size 106)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::int4[])"))
          (dotimes (i size)
            (setf (aref values i) i))
          (pg-exec-prepared con sql `((,values . "_int4")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (eql i (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val int8)")))
        (pg-exec con sql)
        (let* ((size 403)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::int8[])"))
          (dotimes (i size)
            (setf (aref values i) (- i)))
          (pg-exec-prepared con sql `((,values . "_int8")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val DESC"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (eql (- i) (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val float4)")))
        (pg-exec con sql)
        (let* ((size 67)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::float4[])"))
          (dotimes (i size)
            (setf (aref values i) i))
          (pg-exec-prepared con sql `((,values . "_float4")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (pgtest-approx= i (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val float8)")))
        (pg-exec con sql)
        (let* ((size 17)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::float8[])"))
          (dotimes (i size)
            (setf (aref values i) i))
          (pg-exec-prepared con sql `((,values . "_float8")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (pgtest-approx= i (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray"))
      (when-let* ((sql (pgtest-massage con "CREATE TABLE sarray(id SERIAL PRIMARY KEY, val TEXT)")))
        (pg-exec con sql)
        (let* ((size 172)
               (values (make-vector size nil))
               (sql "INSERT INTO sarray(val) SELECT * FROM unnest($1::text[])"))
          (dotimes (i size)
            (setf (aref values i) (format "%04d-value" i)))
          (pg-exec-prepared con sql `((,values . "_text")))
          (pgtest-flush-table con "sarray")
          (let* ((res (pg-exec con "SELECT val FROM sarray ORDER BY val"))
                 (rows (pg-result res :tuples)))
            (dotimes (i size)
              (should (string= (format "%04d-value" i) (cl-first (nth i rows)))))))
        (pg-exec con "DROP TABLE sarray")))))


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
    (when-let* ((sql (pgtest-massage con "CREATE TABLE prep(a INTEGER PRIMARY KEY, b INTEGER)")))
      (pg-exec con sql)
      (dotimes (i 10)
        (pg-exec-prepared con "INSERT INTO prep VALUES($1, $2)"
                          `((,i . "int4") (,(* i i) . "int4"))))
      (pgtest-flush-table con "prep")
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
        (dotimes (_ 1000)
          (let ((v (pcase (random 4)
                     (0 (scalar "SELECT COUNT(*) FROM prep"))
                     (1 (pfp ps1 nil))
                     (2 (pfp ps2 `((0 . "int4"))))
                     (3 (pfp ps3 `((0 . "int4")))))))
            (should (eql v 10))))
        (pg-exec con "DROP TABLE prep")))))


;; https://www.postgresql.org/docs/current/multibyte.html
(defun pg-test-client-encoding (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (row (sql) (pg-result (pg-exec con sql) :tuple 0)))
    (unwind-protect
        (progn 
          ;; (pg-exec con "SET client_encoding TO 'SQL_ASCII'")
          ;; (setf (pgcon-client-encoding con) 'ascii)
          (pg-set-client-encoding con "SQL_ASCII")
          (should (equal "foobles" (scalar "SELECT 'foobles'")))
          (should (eql 'ok (condition-case nil
                               (pg-exec con "SELECT '😏'")
                             (pg-encoding-error 'ok))))
          (should (equal "FOOBLES" (scalar "SELECT 'FOOBLES'")))
          ;; (pg-exec con "SET client_encoding TO 'UTF8'")
          ;; (setf (pgcon-client-encoding con) 'utf-8)
          (pg-set-client-encoding con "UTF8")
          (should (equal "foobles" (scalar "SELECT 'foobles'")))
          (should (equal "föéàµ©" (scalar "SELECT 'föéàµ©'")))
          (should (equal (list "é!à" "more😏than") (row "SELECT 'é!à', 'more😏than'")))
          (should (equal "墲いfooローマ字入力" (scalar "SELECT '墲いfooローマ字入力'")))
          ;; This works with 'iso-latin-1 but not with 'latin-1, due to an Emacs issue.
          ;; (pg-exec con "SET client_encoding TO 'LATIN1'")
          ;; (setf (pgcon-client-encoding con) 'iso-latin-1)
          (pg-set-client-encoding con "LATIN1")
          (should (equal "foobles" (scalar "SELECT 'foobles'")))
          (should (equal "föéàµ" (scalar "SELECT 'föéàµ'")))
          ;; (pg-exec con "SET client_encoding TO 'WIN1250'")
          ;; (setf (pgcon-client-encoding con) 'windows-1250)
          (pg-set-client-encoding con "WIN1250")
          (should (equal "foobles" (scalar "SELECT 'foobles'")))
          (should (equal "ŐÇąěý" (scalar "SELECT 'ŐÇąěý'")))
          ;; (pg-exec con "SET client_encoding TO 'EUC_JP'")
          ;; (setf (pgcon-client-encoding con) 'eucjp-ms)
          (pg-set-client-encoding con "EUC_JP")
          (should (equal "foobles" (scalar "SELECT 'foobles'")))
          (should (equal "あえをビル" (scalar "SELECT 'あえをビル'"))))
      (pg-set-client-encoding con "UTF8"))))


(defun pg-test-procedures (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (pg-function-p con "version"))
    (scalar "DROP FUNCTION IF EXISTS pgtest_difference")
    (let* ((sql "CREATE FUNCTION pgtest_difference(integer, integer) RETURNS integer
                 AS 'select $1 - $2;'
                 LANGUAGE SQL
                 IMMUTABLE
                 RETURNS NULL ON NULL INPUT")
           (res (pg-exec con sql)))
      (should (string-prefix-p "CREATE" (pg-result res :status)))
      (should (pg-function-p con "pgtest_difference"))
      (should (eql 5 (scalar "SELECT * FROM pgtest_difference(105, 100)")))
      ;; Redefining an existing function should trigger an error.
      (should (eql 'ok (condition-case nil
                           (pg-exec con "CREATE FUNCTION pgtest_difference(integer, integer) RETURNS integer
                                         AS 'select - ($2 - $1);'
                                         LANGUAGE SQL
                                         IMMUTABLE
                                         RETURNS NULL ON NULL INPUT")
                         (pg-programming-error 'ok))))
      (pg-exec con "DROP FUNCTION pgtest_difference")
      (should (not (pg-function-p con "pgtest_difference"))))
    (scalar "DROP FUNCTION IF EXISTS pgtest_increment")
    (let* ((sql "CREATE FUNCTION pgtest_increment(val integer) RETURNS integer AS $$
                 BEGIN RETURN val + 1; END; $$
                 LANGUAGE PLPGSQL")
           (res (pg-exec con sql)))
      (should (string-prefix-p "CREATE" (pg-result res :status)))
      (should (pg-function-p con "pgtest_increment"))
      (should (eql -42 (scalar "SELECT pgtest_increment(-43)")))
      (pg-exec con "DROP FUNCTION pgtest_increment")
      (should (not (pg-function-p con "pgtest_increment"))))))

;; Testing for the date/time handling routines.
(defun pg-test-date (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (with-environment-variables (("TZ" "UTC-01:00"))
      (pg-exec con "SET TimeZone = 'UTC-01:00'")
      (pg-exec con "DROP TABLE IF EXISTS date_test")
      (pg-exec con (pgtest-massage con "CREATE TABLE date_test(
           id INTEGER PRIMARY KEY,
           ts TIMESTAMP,
           tstz TIMESTAMPTZ,
           t TIME,
           ttz TIMETZ,
           d DATE)"))
      (unless (member (pgcon-server-variant con) '(cockroachdb))
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
              (pgtest-flush-table con "date_test")
              (should (eql 2 (scalar "SELECT COUNT(*) FROM date_test"))))
          (pg-exec con "DROP TABLE date_test")))
      (unless (member (pgcon-server-variant con) '(cockroachdb))
        (should (equal (scalar "SELECT 'allballs'::time") "00:00:00")))
      (should (equal (scalar "SELECT '2022-10-01'::date")
                     (encode-time (list 0 0 0 1 10 2022))))
      ;; A leap year
      (should (equal (scalar "SELECT '2000-02-29'::date")
                     (encode-time (list 0 0 0 29 2 2000))))
      ;; When casting to DATE, the time portion is truncated
      (should (equal (scalar "SELECT '2063-03-31T22:13:02'::date")
                     (encode-time (list 0 0 0 31 3 2063))))
      ;; Here the hh:mm:ss are taken into account.
      (should (equal (scalar "SELECT '2063-03-31T22:13:02'::timestamp")
                     (encode-time (list 2 13 22 31 3 2063 nil -1 nil))))
      (should (eql 21 (scalar "SELECT EXTRACT (century FROM NOW())")))
      (unless (member (pgcon-server-variant con) '(ydb))
        (message "TZ test: current PostgreSQL timezone is %s" (scalar "SHOW timezone")))
      (message "TZ test: current Emacs timezone is %s" (current-time-zone))
      (message "TZ test: no-DST value is 2010-02-05 14:42:21")
      (let* ((ts (encode-time (list 21 42 14 5 2 2010 nil -1 'wall)))
             (fmt (format-time-string "%Y-%m-%dT%H:%M:%S.%3N%z" ts t)))
        (message "TZ test: encode-time 21 42 14 5 2 2010 nil -1 'wall => %s %s"
                 ts fmt))
      (let ((pg-disable-type-coercion t))
        (message "TZ test: no-DST raw timestamp from PostgreSQL: %s"
                 (scalar "SELECT '2010-02-05 14:42:21'::timestamp")))
      (message "TZ test: no-DST timestamptz from PostgreSQL: %s"
               (scalar "SELECT '2010-02-05 14:42:21'::timestamptz"))
      (message "TZ test: no-DST timestamp from PostgreSQL: %s"
               (scalar "SELECT '2010-02-05 14:42:21'::timestamp"))
      (message "TZ test: no-DST encoded time ZONE=nil = %s"
               (encode-time (list 21 42 14 5 2 2010 nil -1 nil)))
      (message "TZ test: no-DST encoded time UTC-01:00 = %s"
               (encode-time (list 21 42 14 5 2 2010 nil -1 "UTC-01:00")))
      (message "TZ test: no-DST encoded time 'wall = %s"
               (encode-time (list 21 42 14 5 2 2010 nil -1 'wall)))
      (message "TZ test: w/DST value is 2010-06-05 14:42:21")
      (let ((pg-disable-type-coercion t))
        (message "TZ test: w/DST raw timestamp from PostgreSQL: %s"
                 (scalar "SELECT '2010-06-05 14:42:21'::timestamp")))
      (message "TZ test: w/DST timestamptz from PostgreSQL: %s"
               (scalar "SELECT '2010-06-05 14:42:21'::timestamptz"))
      (message "TZ test: w/DST timestamp from PostgreSQL: %s"
               (scalar "SELECT '2010-06-05 14:42:21'::timestamp"))
      (message "TZ test: w/DST encoded time ZONE=nil = %s"
               (encode-time (list 21 42 14 5 6 2010 nil -1 nil)))
      (message "TZ test: w/DST encoded time UTC-01:00 = %s"
               (encode-time (list 21 42 14 5 6 2010 nil -1 "UTC-01:00")))
      (message "TZ test: w/DST encoded time 'wall = %s"
               (encode-time (list 21 42 14 5 6 2010 nil -1 'wall)))
      (should (equal "04:05:06" (scalar "SELECT time without time zone '040506'")))
      ;; In this test, we have ensured that the PostgreSQL session timezone is the same as the
      ;; timezone used by Emacs for encode-time. Passing ZONE=nil means using Emacs' interpretation
      ;; of local time, which should correspond to that of PostgreSQL.
      ;;
      ;; 2025-02: this test is failing on YDB v23.4
      (should (equal (scalar "SELECT '2010-04-05 14:42:21'::timestamp with time zone")
                     ;; SECOND MINUTE HOUR DAY MONTH YEAR IGNORED DST ZONE
                     (encode-time (list 21 42 14 5 4 2010 nil -1 "UTC-01:00"))))
      (should (equal (scalar "SELECT '2010-04-05 14:42:21'::timestamp without time zone")
                     (encode-time (list 21 42 14 5 4 2010 nil -1 nil))))
      (should (equal (scalar "SELECT 'PT42S'::interval") "00:00:42"))
      (should (equal (scalar "SELECT 'PT3H4M42S'::interval") "03:04:42"))
      (should (equal (scalar "SELECT 0 * interval '10 second'") "00:00:00"))
      (should (eql 3 (scalar "SELECT extract(year from '3 years'::interval)")))
      (should (equal (scalar "select '05:00'::time") "05:00:00"))
      (should (equal (scalar "SELECT '04:15:31.445+05'::timetz") "04:15:31.445+05"))
      (should (equal (scalar "SELECT '2001-02-03 04:05:06'::timestamp")
                     (encode-time (list 6 5 4 3 2 2001 nil -1 nil))))
      (should (equal (scalar "SELECT '{2022-10-01,2020-05-06,1975-02-15}'::date[]")
                     (vector (encode-time (list 0 0 0 1 10 2022))
                             (encode-time (list 0 0 0 6 5 2020))
                             (encode-time (list 0 0 0 15 2 1975))))))))


(defun pg-test-numeric (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (eql -1 (scalar "SELECT '-1'::int")))
    (should (eql 128 (scalar "SELECT 128::int2")))
    (should (eql -5 (scalar "SELECT -5::int2")))
    (should (eql 27890 (scalar "SELECT 27890::int4")))
    (should (eql -128 (scalar "SELECT -128::int4")))
    (should (eql 66 (scalar "SELECT 66::int8")))
    (should (eql -1 (scalar "SELECT -1::int8")))
    (should (eql 42 (scalar "SELECT '42'::smallint")))
    (should (eql (scalar "SELECT '-0'::int2") 0))
    (should (eql (scalar "SELECT '-0'::int4") 0))
    (should (eql (scalar "SELECT '-0'::int8") 0))
    ;; (should (string= "-32768" (scalar "SELECT (-1::int2<<15)::text")))
    (should (eql 0 (scalar "SELECT (-32768)::int2 % (-1)::int2")))
    ;; RisingWave doesn't support numeric(x, y) or decimal(x, y).
    (unless (member (pgcon-server-variant con) '(risingwave questdb))
      (should (pgtest-approx= 3.14 (scalar "SELECT 3.14::decimal(10,2) as pi"))))
    ;; CrateDB doesn't support the OID type, nor casting integers to bits.
    (unless (member (pgcon-server-variant con) '(cratedb risingwave materialize octodb yellowbrick datafusion))
      (should (eql 123 (scalar "SELECT 123::oid")))
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
      ;; RisingWave and DataFusion do not implement the bit type
      (unless (member (pgcon-server-variant con) '(risingwave materialize yellowbrick datafusion))
        (should (equal (cl-coerce (vector t nil t nil) 'bool-vector)
                       (scalar "SELECT '1010'::bit(4)"))))
      (unless (member (pgcon-server-variant con) '(cockroachdb risingwave materialize yellowbrick datafusion))
        (should (equal (cl-coerce (vector t nil nil t nil nil nil) 'bool-vector)
                       (scalar "SELECT b'1001000'"))))
      (unless (member (pgcon-server-variant con) '(cratedb risingwave materialize yellowbrick datafusion))
        (should (equal (cl-coerce (vector t nil t t t t) 'bool-vector)
                       (scalar "SELECT '101111'::varbit(6)")))))
    ;; (should (eql 66 (scalar "SELECT 66::money")))
    (should (eql (scalar "SELECT floor(42.3)") 42))
    (unless (member (pgcon-server-variant con) '(ydb))
      (should (eql (scalar "SELECT trunc(43.3)") 43))
      (should (eql (scalar "SELECT trunc(-42.3)") -42)))
    (unless (member (pgcon-server-variant con) '(cockroachdb))
      (should (pgtest-approx= (scalar "SELECT log(100)") 2))
      ;; bignums only supported from Emacs 27.2 onwards
      (unless (member (pgcon-server-variant con) '(cratedb risingwave materialize))
        (when (fboundp 'bignump)
          (should (eql (scalar "SELECT factorial(25)") 15511210043330985984000000)))))
    (unless (member (pgcon-server-variant con) '(materialize))
      (should (pgtest-approx= (scalar "SELECT pi()") 3.1415626)))
    (should (pgtest-approx= (scalar "SELECT '-0'::float4") 0.0))
    (should (pgtest-approx= (scalar "SELECT '-0'::float8") 0.0))
    (should (pgtest-approx= (scalar "SELECT -5.0") -5.0))
    (should (pgtest-approx= (scalar "SELECT 5e-14") 5e-14))
    (should (pgtest-approx= (scalar "SELECT 55.678::float4") 55.678))
    (should (pgtest-approx= (scalar "SELECT 55.678::float8") 55.678))
    (should (pgtest-approx= (scalar "SELECT 55.678::real") 55.678))
    (should (pgtest-approx= (scalar "SELECT 55.678::numeric") 55.678))
    (should (pgtest-approx= (scalar "SELECT -1000000000.123456789") -1000000000.123456789))
    (should (eql 1.0e+INF (scalar "SELECT 'Infinity'::float4")))
    (should (eql -1.0e+INF (scalar "SELECT '-Infinity'::float4")))
    (should (eql 1.0e+INF (scalar "SELECT 'Infinity'::float8")))
    (should (eql -1.0e+INF (scalar "SELECT '-Infinity'::float8")))
    (should (isnan (scalar "SELECT 'NaN'::float4")))
    (should (isnan (scalar "SELECT 'NaN'::float8")))
    (should (eql 1.0e+INF (scalar "SELECT 'Infinity'::numeric")))
    (should (pgtest-approx= (scalar "SELECT 100.0::numeric(30,20) + 500.0::numeric(30,20)") 600.0))
    (should (pgtest-approx= (scalar "SELECT 0.000005::numeric(30,20) + 0.000005::numeric(30,20)") 0.00001))
    (should (eql 42 (scalar "SELECT ceil(41.9)")))
    (should (eql -42 (scalar "SELECT ceil(-42.9)")))
    (should (eql 42 (scalar "SELECT ceil(42::bigint)")))
    (should (pgtest-approx= 1.0 (scalar "SELECT sign(42.0)")))
    (should (pgtest-approx= 1.0 (scalar "SELECT sign(42::bigint)")))
    (should (pgtest-approx= 1.0 (scalar "SELECT sign(42.0::float)")))
    (should (equal pg-null-marker (scalar "SELECT sqrt(NULL)")))
    (should (eql pg-null-marker (scalar "SELECT ceil(NULL)")))
    (should (pgtest-approx= (scalar "SELECT log(10, 10)") 1.0))
    (should (pgtest-approx= (scalar "SELECT log(100.0)") 2.0))
    (should (eql 1 (scalar "SELECT 3 % 2")))
    (should (pgtest-approx= 81 (scalar "SELECT 3::numeric ^ 4::numeric")))
    ;; The cube root operator
    (unless (member (pgcon-server-variant con) '(cratedb materialize))
      (should (pgtest-approx= 3.0 (scalar "SELECT ||/ float8 '27'"))))
    (should (string= (scalar "SELECT 42::decimal::text") "42"))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb risingwave materialize))
      (should (string= (scalar "SELECT macaddr '08002b:010203'") "08:00:2b:01:02:03")))
    (should (eql (scalar "SELECT char_length('foo')") 3))
    (should (string= (scalar "SELECT lower('FOO')") "foo"))
    (should (eql (scalar "SELECT ascii('a')") 97))
    (should (eql (length (scalar "SELECT repeat('Q', 5000)")) 5000))
    (let ((4days (scalar "SELECT interval '1 day' + interval '3 days'")))
      (should (or (string= 4days "4 days")
                  ;; CrateDB prints the result in this way (valid if not hugely helpful)
                  (string= 4days "4 days 00:00:00"))))
    ;; CrateDB returns this as a string "3 days 00:00:00"
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (eql (scalar "SELECT date '2001-10-01' - date '2001-09-28'") 3)))))

(defun pg-test-numeric-range (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
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
      (should (pgtest-approx= 33.33 (nth 2 range)))
      (should (eql ?\) (nth 3 range)))
      (should (pgtest-approx= 66.66 (nth 4 range))))
    (should (pgtest-approx= -40.0 (scalar "SELECT upper(numrange(-50.0,-40.00))")))
    ;; range is unbounded on lower side
    (let ((range (scalar "SELECT numrange(NULL, 2.2)")))
      (should (eql :range (nth 0 range)))
      (should (eql ?\( (nth 1 range)))
      (should (eql nil (nth 2 range)))
      (should (eql ?\) (nth 3 range)))
      (should (pgtest-approx= 2.2 (nth 4 range))))
    (should (equal (list :range ?\[ 42 ?\) nil) (scalar "SELECT int8range(42,NULL)")))
    (should (equal (list :range ?\( nil ?\) nil) (scalar "SELECT numrange(NULL, NULL)")))))


;; https://www.postgresql.org/docs/current/datatype-xml.html#DATATYPE-XML-CREATING
;;
;; We are handling XML as an Emacs Lisp string. PostgreSQL is not always compiled with
;; XML support, so check for that first.
(defun pg-test-xml (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (unless (zerop (scalar "SELECT COUNT(*) FROM pg_type WHERE typname='xml'"))
      (should (string= "<foo attr=\"45\">bar</foo>"
                       (scalar "SELECT xmlparse(CONTENT '<foo attr=\"45\">bar</foo>')")))
      (should (string= (scalar "SELECT xmlforest('abc' AS foo, 123 AS bar)")
                       "<foo>abc</foo><bar>123</bar>"))
      (should (string= "" (scalar "SELECT xmlparse(CONTENT '<?xml version=\"1.0\"?>')")))
      (let* ((res (pg-exec-prepared con "SELECT $1" '(("<foo><bar>45</bar></foo>" . "xml"))))
             (row (pg-result res :tuple 0)))
        (should (eql 5 (cl-search "<bar>" (cl-first row)))))
      (should (cl-search "Foobles" (scalar "SELECT xmlcomment('Foobles')")))
      (should (eql 'ok (condition-case nil
                           (scalar "SELECT xmlparse(CONTENT '<')")
                         (pg-xml-error 'ok))))
      (should (eql 'ok (condition-case nil
                           (scalar "SELECT xmlparse(DOCUMENT '<?xml version=\"1.0\" bizzles=\"bazzles\"?><foo/>')")
                         (pg-xml-error 'ok))))
      (should (eql 'ok (condition-case nil
                           (scalar "SELECT xmlparse(CONTENT '<foo/><bar></baz>')")
                         (pg-xml-error 'ok)))))))


;; https://www.postgresql.org/docs/current/datatype-uuid.html
(defun pg-test-uuid (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0)))
            (scalar/p (sql args) (car (pg-result (pg-exec-prepared con sql args) :tuple 0))))
    (should (string-equal-ignore-case
             "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
             (scalar "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid")))
    (should (string-equal-ignore-case
             "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
             (scalar "SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid")))
    (let ((uuids (scalar "SELECT '{\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\",
                                   \"c4792ecb-c00a-43a2-bd74-5b0ed551c599\"}'::uuid[]")))
      (should (vectorp uuids))
      (should (string-equal-ignore-case (aref uuids 0)  "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
      (should (string-equal-ignore-case (aref uuids 1) "c4792ecb-c00a-43a2-bd74-5b0ed551c599")))
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
     (string-equal-ignore-case
      "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      (scalar/p "SELECT $1" `(("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" . "uuid")))))
    (should
     (string-equal-ignore-case
      ;; PostgreSQL returns the UUID in canonical (lowercase) format, but some variants such as
      ;; QuestDB do not canonicalize.
      "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      (scalar/p "SELECT $1" `(("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11" . "uuid")))))
    (when (pgtest-have-table con "uuidarray")
      (pg-exec con "DROP TABLE uuidarray"))
    (when-let* ((sql (pgtest-massage con "CREATE TABLE uuidarray(id SERIAL PRIMARY KEY, val UUID[])")))
      (pg-exec con sql)
      (pg-exec-prepared con "INSERT INTO uuidarray(val) VALUES($1)"
                        '((["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "c4792ecb-c00a-43a2-bd74-5b0ed551c599"] . "_uuid")))
      (pgtest-flush-table con "uuidarray")
      (let* ((res (pg-exec con "SELECT val FROM uuidarray"))
             (ua (cl-first (pg-result res :tuple 0))))
        (should (string-equal-ignore-case (aref ua 0) "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")))
      (pg-exec con "DROP TABLE uuidarray"))
    ;; New UUIDv7 functionality introduced in PostgreSQL v18
    (when (pg-function-p con "uuidv7")
      (let* ((res (pg-exec con "SELECT uuidv7()"))
             (row (pg-result res :tuple 0))
             (uuid (cl-first row))
             (uuid-rx (rx (= 8 xdigit) (= 3 (seq ?- (= 4 xdigit))) ?- (= 12 xdigit))))
        ;; eg 01987fb8-b258-70fd-a574-1c3f9b89ee21
        (should (string-match uuid-rx uuid))))))



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
  (pg-exec con (pgtest-massage con "CREATE TABLE byteatest(id INT PRIMARY KEY, blob BYTEA)"))
  (pg-exec con "INSERT INTO byteatest VALUES(1, 'warning\\000'::bytea)")
  (pg-exec con "INSERT INTO byteatest VALUES(2, '\\001\\002\\003'::bytea)")
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (eql 0 (length (scalar "SELECT decode('', 'hex')"))))
    (should (eql 0 (scalar "SELECT length(''::bytea)")))
    (should (equal (byte-to-string 0) (scalar "SELECT '\\000'::bytea")))
    (should (equal (byte-to-string ?') (scalar "SELECT ''''::bytea")))
    (should (equal (decode-hex-string "DEADBEEF") (scalar "SELECT '\\xDEADBEEF'::bytea")))
    (should (equal (string 1 3 5) (scalar "SELECT '\\001\\003\\005'::bytea")))
    (should (equal (decode-hex-string "123456789a00bcde")
                   (scalar "SELECT '\\x123456'::bytea || '\\x789a00bcde'::bytea")))
    ;; CockroachDB is returning an encoded hex string from sha256() instead of an integer.
    (unless (member (pgcon-server-variant con) '(cockroachdb))
      (should (equal (secure-hash 'sha256 "foobles")
                     (encode-hex-string (scalar "SELECT sha256('foobles'::bytea)")))))
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
    (should (eql 400 (scalar "SELECT nextval('foo_seq')")))
    (unless (member (pgcon-server-variant con) '(yugabyte))
      (should (eql 400 (scalar "SELECT last_value FROM pg_sequences WHERE sequencename='foo_seq'"))))
    (should (eql 420 (scalar "SELECT nextval('foo_seq')")))
    (should (eql 440 (scalar "SELECT nextval('foo_seq')")))
    (pg-exec con "DROP SEQUENCE foo_seq")
    ;; CockroachDB does not implement the CYCLE option on sequences.
    (unless (member (pgcon-server-variant con) '(cockroachdb))
      (pg-exec con "DROP SEQUENCE IF EXISTS test_seq_nocycle")
      (pg-exec con "CREATE SEQUENCE test_seq_nocycle START 1 INCREMENT 1 MAXVALUE 2 NO CYCLE")
      (should (eql 1 (scalar "SELECT nextval('test_seq_nocycle')")))
      (should (eql 2 (scalar "SELECT nextval('test_seq_nocycle')")))
      (should (eql 'ok (condition-case nil
                           (scalar "SELECT nextval('test_seq_nocycle')")
                         (pg-sequence-limit-exceeded 'ok)
                         ;; CedarDB reports pg-numeric-value-out-of-range
                         (pg-numeric-value-out-of-range 'ok))))
      (pg-exec con "DROP SEQUENCE test_seq_nocycle")
      (pg-exec con "DROP SEQUENCE IF EXISTS test_seq_cycle")
      (pg-exec con "CREATE SEQUENCE test_seq_cycle START 1 INCREMENT 1 MAXVALUE 2 CYCLE")
      (should (eql 1 (scalar "SELECT nextval('test_seq_cycle')")))
      (should (eql 2 (scalar "SELECT nextval('test_seq_cycle')")))
      ;; should cycle
      (should (eql 1 (scalar "SELECT nextval('test_seq_cycle')")))
      (pg-exec con "DROP SEQUENCE test_seq_cycle"))
    (pg-exec con "DROP SEQUENCE IF EXISTS test_seq_setval")
    (pg-exec con "CREATE SEQUENCE test_seq_setval")
    (should (eql 50 (scalar "SELECT setval('test_seq_setval', 50)")))
    (should (eql 50 (scalar "SELECT currval('test_seq_setval')")))
    (should (eql 51 (scalar "SELECT nextval('test_seq_setval')")))
    (pg-exec con "DROP SEQUENCE test_seq_setval")))

(defun pg-test-array (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (equal (vector 7 8) (scalar "SELECT ARRAY[7,8]")))
    (should (equal (vector 9 10 11) (scalar "SELECT '{9,10,11}'::int[]")))
    (should (equal (vector 1234) (scalar "SELECT ARRAY[1234::int2]")))
    (should (equal (vector -3456) (scalar "SELECT ARRAY[-3456::int4]")))
    (should (equal (vector 9987) (scalar "SELECT ARRAY[9987::int8]")))
    (should (equal (vector 2 8) (scalar "SELECT ARRAY[2,8]")))
    (should (equal (vector) (scalar "SELECT ARRAY[]::integer[]")))
    (should (equal (vector) (scalar "SELECT '{}'::int2[]")))
    (should (equal (vector) (scalar "SELECT '{}'::int4[]")))
    (should (equal (vector) (scalar "SELECT '{}'::int8[]")))
    (should (equal (vector) (scalar "SELECT '{}'::char[]")))
    (should (equal (vector) (scalar "SELECT '{}'::varchar[]")))
    (should (equal (vector) (scalar "SELECT '{}'::text[]")))
    (should (equal (vector) (scalar "SELECT '{}'::bool[]")))
    (should (equal (vector) (scalar "SELECT '{}'::float4[]")))
    (should (equal (vector) (scalar "SELECT '{}'::float8[]")))
    (should (equal (vector pg-null-marker) (scalar "SELECT ARRAY[NULL]")))
    (should (equal (vector 1 pg-null-marker 55)
                   (scalar "SELECT ARRAY[1,NULL,55]")))
    (should (equal (vector t pg-null-marker nil)
                   (scalar "SELECT ARRAY[true,NULL,false]")))
    (should (equal (vector ?A pg-null-marker ?Z)
                   (scalar "SELECT ARRAY['A'::char,NULL,'Z'::char]")))
    (should (equal (vector "foo" pg-null-marker "bar")
                   (scalar "SELECT ARRAY['foo',NULL,'bar']")))
    (let ((vec (scalar "SELECT ARRAY[3.14, NULL, 69.420]")))
      (should (floatp (aref vec 0)))
      (should (equal pg-null-marker (aref vec 1))))
    (unless (member (pgcon-server-variant con) '(cratedb cockroachdb cedardb spanner))
      (should (equal (vector "AB1234" "4321BA") (scalar "SELECT '{\"AB1234\",\"4321BA\"}'::bpchar[]"))))
    (let ((vec (scalar "SELECT ARRAY[3.14::float]")))
      (should (floatp (aref vec 0)))
      (should (pgtest-approx= 3.14 (aref vec 0))))
    (let ((vec (scalar "SELECT ARRAY[CAST(3.14 AS DOUBLE PRECISION)]")))
      (should (floatp (aref vec 0)))
      (should (pgtest-approx= 3.14 (aref vec 0))))
    (should (equal (vector 4 20) (scalar "SELECT ARRAY[4] || 20")))
    (should (eql 6 (scalar "SELECT array_length('{1,2,3,4,5,6}'::int4[], 1)")))
    (should (equal (vector 42) (scalar "SELECT array_agg(42)")))
    (should (equal (vector 45 67 89) (scalar "SELECT '{45,67,89}'::smallint[]")))
    (should (equal (vector t nil t nil t)
                   (scalar "SELECT '{true, false, true, false, true}'::bool[]")))
    ;; Risingwave doesn't implement the CHAR type.
    (unless (member (pgcon-server-variant con) '(risingwave))
      (should (equal (vector ?A ?z ?5) (scalar "SELECT '{A,z,5}'::char[]")))
      (should (equal (vector ?x ?Y) (scalar "SELECT '{\"x\", \"Y\"}'::char[]")))
      ;; this is returning _bpchar.
      (should (equal (vector ?a ?b ?c) (scalar "SELECT CAST('{a,b,c}' AS CHAR[])"))))
    (should (equal (vector "foo" "bar") (scalar "SELECT '{foo, bar}'::text[]")))
    (should (equal (vector 1 pg-null-marker 3) (scalar "SELECT ARRAY[1, NULL, 3]")))
    (let ((res (scalar "SELECT ARRAY[4.5, NULL, 6.0, 7.99999]")))
      (should (vectorp res))
      (should (eql 4 (length res)))
      (should (equal pg-null-marker (aref res 1))))
    (let ((res (scalar "SELECT array_remove(ARRAY[1,2,-3], 10)")))
      (should (equal (vector 1 2 -3) res)))
    (let ((res (scalar "SELECT array_prepend(42, ARRAY[7,8,9])")))
      (should (eql (length res) 4))
      (should (eql (aref res 0) 42)))
    (should (equal (vector 2 3 4) (scalar "SELECT a[2:] FROM (SELECT '{1,2,3,4}'::integer[] AS a)")))
    (should (equal (vector) (scalar "SELECT a[10:] FROM (SELECT '{1,2,3,4}'::integer[] AS a)")))
    (should (string= "{1,2}" (scalar "SELECT concat(NULL, ARRAY[1, 2])")))
    (should (string= "{1}{2,69}" (scalar "SELECT concat(ARRAY[1::bigint], ARRAY[2, 69::int2])")))
    (should (string= "69{420}" (scalar "SELECT concat(69, ARRAY[420])")))
    (let* ((res (pg-exec-prepared con "SELECT $1" '(([1 2 3] . "_int4"))))
           (row (pg-result res :tuple 0)))
      (should (equal (vector 1 2 3) (cl-first row))))
    (let ((vec (scalar "SELECT ARRAY[44.3, 8999.5]")))
      (should (equal 2 (length vec)))
      (should (pgtest-approx= 44.3 (aref vec 0)))
      (should (pgtest-approx= 8999.5 (aref vec 1))))
    (should (equal 42 (scalar "SELECT unnest(ARRAY[42])")))))

;; TODO: we do not currently handle multidimension arrays correctly
;; (should (equal (vector (vector 4 5) (vector 6 7))
;;                (scalar "SELECT '{{4,5},{6,7}}'::int8[][]")))))


;; Test functionality related to "COMMENT ON TABLE" and "COMMENT ON COLUMN"
(defun pg-test-comments (con)
  (pg-exec con "DROP TABLE IF EXISTS comment_test")
  (pg-exec con "CREATE TABLE comment_test(cola INTEGER, colb VARCHAR)")
  (should (null (pg-table-comment con "comment_test")))
  (dolist (cmt (list "Easy" "+++---" "éàÖ🫎"))
    (setf (pg-table-comment con "comment_test") cmt)
    (should (string= cmt (pg-table-comment con "comment_test"))))
  (setf (pg-table-comment con "comment_test") nil)
  (should (null (pg-table-comment con "comment_test")))
  (dolist (cmt (list "Simple" "!!§§??$$$$$$$$$$$$$$$" "éàÖ🫎"))
    (setf (pg-column-comment con "comment_test" "cola") cmt)
    (should (string= cmt (pg-column-comment con "comment_test" "cola")))
    (setf (pg-column-comment con "comment_test" "colb") cmt)
    (should (string= cmt (pg-column-comment con "comment_test" "colb"))))
  (setf (pg-column-comment con "comment_test" "cola") nil)
  (should (null (pg-column-comment con "comment_test" "cola")))
  (setf (pg-column-comment con "comment_test" "colb") nil)
  (should (null (pg-column-comment con "comment_test" "colb")))
  (pg-exec con "DROP TABLE comment_test")
  ;; Now test for a qualified-name with a custom schema (this will exercise different code paths for
  ;; some PostgreSQL variants).
  (pg-exec con "DROP SCHEMA IF EXISTS pgeltestschema CASCADE")
  (pg-exec con "CREATE SCHEMA pgeltestschema")
  (pg-exec con "CREATE TABLE pgeltestschema.comment_test(cola INTEGER, colb VARCHAR)")
  (let ((tname (make-pg-qualified-name :schema "pgeltestschema" :name "comment_test")))
    (should (null (pg-table-comment con tname)))
    (dolist (cmt (list "Easy" "ç+++---" "éàÖ🐘"))
      (setf (pg-table-comment con tname) cmt)
      (should (string= cmt (pg-table-comment con tname))))
    (setf (pg-table-comment con tname) nil)
    (should (null (pg-table-comment con tname)))
    (dolist (cmt (list "Simple" "!!§§??$$$$$$$$$$$$$$$" "🐘🫎éàÖ"))
      (setf (pg-column-comment con tname "cola") cmt)
      (should (string= cmt (pg-column-comment con tname "cola")))
      (setf (pg-column-comment con tname "colb") cmt)
      (should (string= cmt (pg-column-comment con tname "colb"))))
    (setf (pg-column-comment con tname "cola") nil)
    (should (null (pg-column-comment con tname "cola")))
    (setf (pg-column-comment con tname "colb") nil)
    (should (null (pg-column-comment con tname "colb")))
    (pg-exec con "DROP TABLE pgeltestschema.comment_test")
    (pg-exec con "DROP SCHEMA pgeltestschema")))

(defun pg-test-metadata (con)
  (unless (member (pgcon-server-variant con) '(xata cedardb))
    (pg-exec con "SET work_mem TO '2MB'")
    (pg-exec con "EXPLAIN (COSTS OFF) SELECT 42")
    (pg-exec con "RESET work_mem"))
  ;; Check that the pg_user table exists and that we can parse the name type
  (let* ((res (pg-exec con "SELECT usename FROM pg_user"))
         (users (pg-result res :tuples)))
    (should (> (length users) 0)))
  ;; CrateDB does not implement SERIAL, because that would make it difficult to allow different
  ;; nodes to ingest data in parallel.
  (unless (member (pgcon-server-variant con) '(cratedb))
    (pg-exec con "DROP TABLE IF EXISTS coldefault")
    (pg-exec con "CREATE TABLE coldefault(id SERIAL PRIMARY KEY, comment VARCHAR)")
    ;; note that the id column has a DEFAULT value due to the SERIAL (this is not present for a
    ;; GENERATED ALWAYS AS INTEGER column).
    (pg-exec con "INSERT INTO coldefault(comment) VALUES ('foobles')")
    (should (pg-column-default con "coldefault" "id"))
    (should (pg-column-autogenerated-p con "coldefault" "id"))
    (should (not (pg-column-default con "coldefault" "comment")))
    (should (not (pg-column-autogenerated-p con "coldefault" "comment")))
    (pg-exec con "DROP TABLE coldefault"))
  ;; GENERATED ALWAYS support was implemented in v12 it seems
  (when (and (not (member (pgcon-server-variant con) '(questdb)))
             (> (pgcon-server-version-major con) 11))
    (pg-exec con "DROP TABLE IF EXISTS colgen_id")
    (pg-exec con "CREATE TABLE colgen_id(id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, comment TEXT)")
    (pg-exec con "INSERT INTO colgen_id(comment) VALUES('bizzles')")
    ;; A generated column does not have a DEFAULT, in the PostgreSQL sense
    (should (not (pg-column-default con "colgen_id" "id")))
    (should (pg-column-autogenerated-p con "colgen_id" "id"))
    (should (not (pg-column-default con "colgen_id" "comment")))
    (should (not (pg-column-autogenerated-p con "colgen_id" "comment")))
    (pg-exec con "DROP TABLE colgen_id")
    (pg-exec con "DROP TABLE IF EXISTS colgen_expr")
    (pg-exec con "CREATE TABLE colgen_expr(count INTEGER PRIMARY KEY, double INTEGER GENERATED ALWAYS AS (count*2) STORED)")
    (pg-exec con "INSERT INTO colgen_expr(count) VALUES(5)")
    (should (not (pg-column-default con "colgen_expr" "double")))
    (should (not (pg-column-default con "colgen_expr" "count")))
    (should (not (pg-column-autogenerated-p con "colgen_expr" "count")))
    (should (pg-column-autogenerated-p con "colgen_expr" "double"))
    (pg-exec con "DROP TABLE colgen_expr")))

;; Schemas for qualified names such as public.tablename.
(defun pg-test-schemas (con)
  (let ((res (pg-exec con "CREATE SCHEMA IF NOT EXISTS custom")))
    (should (string-prefix-p "CREATE" (pg-result res :status))))
  (let* ((sql (pgtest-massage con "CREATE TABLE IF NOT EXISTS custom.newtable(id INT4 PRIMARY KEY)"))
         (res (pg-exec con sql)))
    (should (string-prefix-p "CREATE" (pg-result res :status))))
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
  (let* ((sql (pgtest-massage con "CREATE TABLE IF NOT EXISTS %s.%s(id INT4 PRIMARY KEY)"
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
  (let* ((sql (pgtest-massage con "CREATE TABLE IF NOT EXISTS %s.%s(id INT4 PRIMARY KEY)"
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
         (sql (pgtest-massage con "CREATE TABLE IF NOT EXISTS %s(id INT4 PRIMARY KEY)"
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
  ;; SQL function call using a parameter and our printing support for qualified names. CockroachDB
  ;; has no support for pg_total_relation_size().
  (unless (member (pgcon-server-variant con) '(cockroachdb))
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
      (should (<= 0 size 10000000))))
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
    (pg-exec con "DROP TYPE IF EXISTS FRUIT")
    (pg-exec con "CREATE TYPE FRUIT AS ENUM('banana', 'orange', 'apple', 'pear')")
    (let* ((res (pg-exec con "SELECT 'apple'::fruit"))
           (attr (pg-result res :attributes)))
      (should (string= "apple" (car (pg-result res :tuple 0))))
      (should (string= "fruit" (caar attr))))
    (pcase (pgcon-server-variant con)
      ;; CockroachDB does not implement DROP TYPE CASCADE
      ('cockroachdb
       (pg-exec con "DROP TYPE IF EXISTS rating"))
      (_
       (pg-exec con "DROP TYPE IF EXISTS rating CASCADE")))
    (pg-exec con "CREATE TYPE rating AS ENUM('ungood', 'good', 'plusgood',"
             "'doubleplusgood', 'plusungood', 'doubleplusungood')")
    (pg-exec con "DROP TABLE IF EXISTS act")
    (pg-exec con (pgtest-massage con "CREATE TABLE act(name TEXT PRIMARY KEY, value RATING)"))
    (pg-exec con "INSERT INTO act VALUES('thoughtcrime', 'doubleplusungood')")
    (pg-exec con "INSERT INTO act VALUES('thinkpol', 'doubleplusgood')")
    (pg-exec-prepared con "INSERT INTO act VALUES('blackwhite', $1)" `(("good" . "rating")))
    (message "Rating plusgood is %s" (scalar "SELECT 'plusgood'::rating"))
    (pg-exec con "DROP TABLE act")
    (pg-exec con "DROP TYPE rating")))


;; https://www.postgresql.org/docs/15/functions-json.html
(defun pg-test-json (con)
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (should (eql 42 (scalar "SELECT to_json(42)")))
    (should (eql -56 (scalar "SELECT CAST ('-56' as json)")))
    (should (equal pg-null-marker (scalar "SELECT JSON(NULL)")))
    (let ((dct (scalar "SELECT JSON('{ \"a\" : 1 }')")))
      (should (hash-table-p dct))
      (should (eql 1 (gethash "a" dct))))
    (unless (member (pgcon-server-variant con) '(alloydb))
      (when (>= (pgcon-server-version-major con) 17)
        (should (pgtest-approx= 155.6 (scalar "SELECT json_scalar(155.6)")))
        (should (string= "155.6" (scalar "SELECT json_scalar('155.6')")))
        (should (string= "144" (scalar "SELECT json_serialize('144')")))))
    (let ((json (scalar "SELECT '{}'::json")))
      (should (eql 0 (hash-table-count json))))
    (let ((json (scalar "SELECT '[5,7]'::json")))
      (should (eql 5 (aref json 0))))
    (let ((json (scalar "SELECT '[5,7]'::jsonb")))
      (should (eql 5 (aref json 0))))
    (let ((json (scalar "SELECT '[66.7,-42.0,8]'::json")))
      (should (pgtest-approx= 66.7 (aref json 0)))
      (should (pgtest-approx= -42.0 (aref json 1))))
    (let ((json (scalar "SELECT '[66.7,-42.0,8]'::jsonb")))
      (should (pgtest-approx= 66.7 (aref json 0)))
      (should (pgtest-approx= -42.0 (aref json 1))))
    (let ((json (scalar "SELECT '{\"a\":null,\"b\":[1,null,3]}'::jsonb")))
      (should (eql 2 (hash-table-count json)))
      (should (equal :null (gethash "a" json)))
      (should (equal :null (aref (gethash "b" json) 1))))
    (should (string= "number" (scalar "SELECT json_typeof('123')")))
    (should (string= "array" (scalar "SELECT json_typeof('[]')")))
    (should (string= "boolean" (scalar "SELECT json_typeof('true')")))
    (should (eql 0 (scalar "SELECT json_array_length('[]')")))
    (should (eql 10 (scalar "SELECT json_array_length('[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]')")))
    (should (pgtest-approx= 3.14159 (scalar "SELECT to_jsonb(3.14159)")))
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
      (unless (member (pgcon-server-variant con) '(cockroachdb))
        (let ((json (scalar "SELECT json_object_agg(42, 66)")))
          (should (eql 66 (gethash "42" json)))))
      (let ((json (scalar "SELECT '{\"a\":1,\"b\":-22}'::json")))
        (should (eql 1 (gethash "a" json)))
        (should (eql -22 (gethash "b" json))))
      (let ((json (scalar "SELECT '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json")))
        (should (string= "bar" (gethash "b" (aref json 1)))))
      (let ((json (scalar "SELECT '{\"a\": [0,1,2,null]}'::json")))
        (should (eql 2 (aref (gethash "a" json) 2)))))
    (let ((json (scalar "SELECT '{\"unicode\":\"😀\"}'::jsonb")))
      (should (eql 1 (hash-table-count json)))
      (should (string= "😀" (gethash "unicode" json))))
    (when (> (pgcon-server-version-major con) 11)
      (unless (member (pgcon-server-variant con) '(cockroachdb))
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
          (should (eql 5 (cl-first row)))))
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
	  (should (equal (vector "pg-el" :null 42) (cl-first row))))))))



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
      (pg-exec con (pgtest-massage con "CREATE TABLE hstored(id INT8 PRIMARY KEY, meta HSTORE)"))
      (dotimes (i 10)
        (let ((hs (make-hash-table :test #'equal)))
          (puthash (format "foobles-%d" i) (format "bazzles-%d" i) hs)
          (puthash (format "a%d" (1+ i)) (format "%d" (- i)) hs)
          (puthash (format "föéê-%d" i) (format "bâçé-%d" i) hs)
          (puthash (format "a👿%d" (1+ i)) (format "%d" (- i)) hs)
          (pg-exec-prepared con "INSERT INTO hstored(id,meta) VALUES ($1, $2)"
                            `((,i . "int8") (,hs . "hstore")))))
      (pgtest-flush-table con "hstored")
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
  (cl-flet ((scalar (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
    (pg-vector-setup con)
    ;; We test for the presence of vector functionality because some variants like CedarDB implement
    ;; the vector type, but trigger an error when executing "CREATE EXTENSION vector".
    (unless (zerop (scalar "SELECT COUNT(*) FROM pg_type WHERE typname='vector'"))
      (message "Testing pgvector support")
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
      ;; <+> operator is not supported in CockroachDB
      (unless (member (pgcon-server-variant con) '(cockroachdb))
        (let ((d (scalar "SELECT '[1,2,3]'::vector <+> '[4,5,6]'::vector")))
          (should (eql 9 d))))
      (let ((d (scalar "SELECT l1_distance('[1,2,3]'::vector, '[4,5,6]'::vector)")))
        (should (eql 9 d)))
      (let ((d (scalar "SELECT '[1,1,1,1]'::vector <=> '[2,2,2,2]'::vector")))
        (should (eql 0 d)))
      (let ((d (scalar "SELECT cosine_distance('[1,1,1,1]'::vector, '[2,2,2,2]'::vector)")))
        (should (eql 0 d)))
      (let ((d (scalar "SELECT '[1,2,3]'::vector <#> '[4,5,6]'::vector")))
        (should (eql -32 d)))
      (let ((d (scalar "SELECT inner_product('[1,2,3]'::vector, '[4,5,6]'::vector)")))
        (should (eql 32 d)))
      (when (pg-function-p con "gen_random_uuid")
        (pg-exec con "DROP TABLE IF EXISTS items")
        (let ((sql (pgtest-massage con "CREATE TABLE items (
               id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
               embedding vector(4))")))
          (pg-exec con sql))
        (dotimes (_ 1000)
          (let ((new (vector (random 55) (random 66) (random 77) (random 88))))
            (pg-exec-prepared con "INSERT INTO items(embedding) VALUES($1)"
                              `((,new . "vector")))))
        (let ((res (pg-exec con "SELECT embedding FROM items ORDER BY embedding <-> '[1,1,1,1]' LIMIT 1")))
          (message "PGVECTOR> closest = %s" (car (pg-result res :tuple 0))))
        (pg-exec con "DROP TABLE items")))))

;; Testing support for the tsvector type used for full text search.
;; See https://www.postgresql.org/docs/current/datatype-textsearch.html
(defun pg-test-tsvector (con)
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
      (should (cl-find (make-pg-ts :lexeme "Rats") tsvec :test #'equal)))
    (pg-exec con "DROP TABLE IF EXISTS documents")
    (pg-exec con "CREATE TABLE documents(id SERIAL PRIMARY KEY, content TEXT, cvec tsvector)")
    (dolist (phrase (list "PostgreSQL is a powerful, open-source database system."
                          "Full-text search in PostgreSQL is efficient and scalable."
                          "ts-vector support provides reasonably good search functionality."))
      (pg-exec-prepared con "INSERT INTO documents(content, cvec) VALUES ($1,to_tsvector($1))"
                        `((,phrase . "text"))))
    (pg-exec con "CREATE INDEX idx_content_vector ON documents USING GIN (cvec)")
    ;; Query using tsvector, rank results with ts_rank, and leverage the GIN index
    (let* ((sql "SELECT id, content, ts_rank(cvec, to_tsquery('english', 'PostgreSQL & search')) AS rank
                 FROM documents
                 WHERE cvec @@ to_tsquery('english', 'PostgreSQL & search')
                 ORDER BY rank DESC")
           (res (pg-exec con sql))
           (best (pg-result res :tuple 0)))
      (should (cl-search "efficient" (cl-second best))))
    (pg-exec con "DROP TABLE documents")))

;; Specific tests for the VectorChord BM25 extension.
;;
;; https://github.com/tensorchord/VectorChord-bm25/
(defun pg-test-bm25 (con)
  (when (pg-setup-bm25 con)
    (message "Testing Vectorchord BM25 extension support")
    (should (member "bm25_catalog" (pg-schemas con)))
    (let* ((sql "SELECT tokenize('A quick brown fox jumps over the lazy dog.', 'Bert')")
           (res (pg-exec con sql))
           (out (cl-first (pg-result res :tuple 0))))
      ;; or the form "{2474:1, 2829:1, 3899:1, 4248:1, 4419:1, 5376:1, 5831:1}"
      (should (eql ?{ (aref out 0)))
      (should (eql ?} (aref out (1- (length out))))))
    (pg-exec con "DROP TABLE IF EXISTS documents")
    (let* ((sql "CREATE TABLE documents(id SERIAL PRIMARY KEY, passage TEXT, embedding bm25vector)")
           (res (pg-exec con sql)))
      (should (string-prefix-p "CREATE" (pg-result res :status))))
    (dolist (text (list "PostgreSQL is a powerful, open-source object-relational database system. It has over 15 years of active development."
                        "Full-text search is a technique for searching in plain-text documents or textual database fields. PostgreSQL supports this with tsvector."
                        "BM25 is a ranking function used by search engines to estimate the relevance of documents to a given search query."
                        "PostgreSQL provides many advanced features like full-text search, window functions, and more."
                        "Search and ranking in databases are important in building effective information retrieval systems."
                        "The BM25 ranking algorithm is derived from the probabilistic retrieval framework."
                        "Full-text search indexes documents to allow fast text queries. PostgreSQL supports this through its GIN and GiST indexes."
                        "The PostgreSQL community is active and regularly improves the database system."
                        "Relational databases such as PostgreSQL can handle both structured and unstructured data."
                        "Effective search ranking algorithms, such as BM25, improve search results by understanding relevance."))
      (pg-exec-prepared con "INSERT INTO documents(passage) VALUES ($1)"
                        `((,text . "text"))))
    (pg-exec con "UPDATE documents SET embedding = tokenize(passage, 'Bert')")
    (pg-exec con "CREATE INDEX documents_embedding_bm25 ON documents USING bm25 (embedding bm25_ops)")
    (let* ((sql "SELECT id, passage, embedding <&> to_bm25query('documents_embedding_bm25', 'PostgreSQL', 'Bert') AS rank
                 FROM documents
                 ORDER BY rank
                 LIMIT 3")
           (row (pg-result (pg-exec con sql) :tuple 0)))
      (should (cl-search "PostgreSQL community" (cl-second row))))))

(defun pg-test-geometric (con)
  (cl-labels ((row (query args) (pg-result (pg-exec-prepared con query args) :tuple 0))
              (scalar (query args) (car (row query args))))
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
      (should (pgtest-approx= 33e4 (car p4)))
      (should (<= -35000 (cdr p4) 31000)))
    (let ((p5 (pg--point-parser "(a,b)" nil)))
      (should (eql nil p5)))
    (should (eql nil (pg--point-parser "" nil)))
    (let ((p7 (pg--point-parser "(3,4)" nil)))
      (should (eql 3 (car p7)))
      (should (eql 4 (cdr p7))))
    (let ((p8 (pg--point-parser "(12.1,4e-4) " nil)))
      (should (pgtest-approx= 4 (* 1e4 (cdr p8)))))
    (let ((p9 (pg--point-parser " (55,7866677)" nil)))
      (should (eql 55 (car p9))))
    (let ((p10 (pg--point-parser "(22.6,6) " nil)))
      (should (eql 6 (cdr p10))))
    (let ((point (scalar "SELECT '(82,91.0)'::point" nil)))
      (should (consp point))
      (should (eql 82 (car point)))
      (should (pgtest-approx= 91.0 (cdr point))))
    (when (pg-function-p con "gen_random_uuid")
      (pg-exec con "DROP TABLE IF EXISTS with_point")
      (pg-exec con (pgtest-massage con "CREATE TABLE with_point(
            id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
            p POINT)"))
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
      (pg-exec con (pgtest-massage con "CREATE TABLE with_line(
           id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
           ln LINE)"))
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
        (should (pgtest-approx= 4e1 (cdr (aref lseg 1)))))
      (pg-exec con "DROP TABLE IF EXISTS with_lseg")
      (pg-exec con (pgtest-massage con "CREATE TABLE with_lseg(
             id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
             ls LSEG)"))
      (pg-exec con "INSERT INTO with_lseg(ls) VALUES('[(4,5), (6.6,7.7)]')")
      (let* ((ls (vector (cons 2 3) (cons 55.5 66.6)))
             (res (pg-exec-prepared con "SELECT $1" `((,ls . "lseg"))))
             (ls (cl-first (pg-result res :tuple 0))))
        (should (eql 2 (car (aref ls 0))))
        (should (eql 3 (cdr (aref ls 0))))
        (should (pgtest-approx= 55.5 (car (aref ls 1)))))
      (pg-exec con "DROP TABLE with_lseg")
      (let ((box (pg--box-parser "(4,5), (-66,-77e0) " nil)))
        (should (eql 4 (car (aref box 0))))
        (should (eql -66 (car (aref box 1)))))
      (pg-exec con "DROP TABLE IF EXISTS with_box")
      (pg-exec con (pgtest-massage con "CREATE TABLE with_box(
              id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
              bx BOX)"))
      (pg-exec con "INSERT INTO with_box(bx) VALUES('(33.3,5),(5,-67e1)')")
      (let* ((bx (vector (cons 2 3) (cons 55.6 -23.2)))
             (res (pg-exec-prepared con "SELECT $1" `((,bx . "box"))))
             (bx (cl-first (pg-result res :tuple 0))))
        ;; the box corners are output in the order upper-right, lower-left
        (should (pgtest-approx= 55.6 (car (aref bx 0))))
        (should (eql 3 (cdr (aref bx 0))))
        (should (pgtest-approx= -23.2 (cdr (aref bx 1)))))
      (pg-exec con "DROP TABLE with_box")
      (let* ((path (pg--path-parser "[(4,5),(6,7), (55e1,66.1),(0,0) ]" nil))
             (points (pg-geometry-path-points path)))
        (should (eql :open (pg-geometry-path-type path)))
        (should (eql 4 (length points)))
        (should (eql 7 (cdr (nth 1 points)))))
      (pg-exec con "DROP TABLE IF EXISTS with_path")
      (pg-exec con (pgtest-massage con "CREATE TABLE with_path(
             id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
             pt PATH)"))
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
      (pg-exec con (pgtest-massage con "CREATE TABLE with_polygon(
               id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
               pg POLYGON)"))
      (pg-exec con "INSERT INTO with_polygon(pg) VALUES('((3,4),(5,6),(44.4,55.5))')")
      (let* ((pg (make-pg-geometry-polygon :points '((2 . 3) (3 . 4) (4 . 5) (6.6 . 7.77))))
             (res (pg-exec-prepared con "SELECT $1" `((,pg . "polygon"))))
             (pg (cl-first (pg-result res :tuple 0)))
             (points (pg-geometry-polygon-points pg)))
        (should (eql 4 (length points)))
        (should (eql 3 (cdr (cl-first points))))
        (should (pgtest-approx= 7.77 (cdr (car (last points))))))
      (pg-exec con "DROP TABLE with_polygon"))))

;; PostGIS parsing tests. These tests require the geosop commandline utility to be installed.
(defun pg-test-gis (con)
  (cl-labels ((row (query) (pg-result (pg-exec con query) :tuple 0))
              (scalar (query) (car (row query))))
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
      (should (eql 20 (scalar "SELECT st_distance('POINT (10 20)'::geometry, 'POINT (30 20)'::geometry)")))
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
    (with-temp-buffer
      (pg-copy-to-buffer con "COPY (values (1, 'hello'), (2, 'world')) TO STDOUT" (current-buffer))
      (should (string= "1\thello\n2\tworld\n" (buffer-string))))
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
  (let ((r1 (pg-exec con (pgtest-massage con "CREATE TABLE resulttest (a INT PRIMARY KEY, b VARCHAR(4))")))
        (r2 (pg-exec con "INSERT INTO resulttest VALUES (3, 'zae')"))
        (r3 (pg-exec con "INSERT INTO resulttest VALUES (66, 'poiu')"))
        (_ (pgtest-flush-table con "resulttest"))
        (r4 (pg-exec con "SELECT * FROM resulttest"))
        (r5 (pg-exec con "UPDATE resulttest SET b='foob' WHERE a=66"))
        (_ (pgtest-flush-table con "resulttest"))
        (r6 (pg-exec con "SELECT b FROM resulttest WHERE a=66"))
        (r7 (pg-exec con "DROP TABLE resulttest"))
        (r8 (pg-exec con "SELECT generate_series(1, 10)")))
    (message "==============================================")
    (message "status of CREATE is %s" (pg-result r1 :status))
    ;; CrateDB returns "CREATE 1" instead of "CREATE TABLE"...
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (string= "CREATE TABLE" (pg-result r1 :status))))
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
    ;; With PostgreSQL the status is "DROP TABLE"; with CedarDB it is "DROP".
    (unless (member (pgcon-server-variant con) '(cratedb))
      (should (string-prefix-p "DROP" (pg-result r7 :status))))
    (should (eql (length (pg-result r8 :tuples)) 10))
    (message "=============================================="))
   (let ((res (pg-exec con "SELECT 7 UNION SELECT 8 ORDER BY 1")))
     (should (equal '((7) (8)) (pg-result res :tuples))))
   (let ((res (pg-exec con "SELECT 1,2,3,'soleil'")))
     (should (equal '(1 2 3 "soleil") (pg-result res :tuple 0))))
   (let ((res (pg-exec con "SELECT 42 as z")))
     (should (string= "z" (caar (pg-result res :attributes)))))
   (let* ((res (pg-exec con "SELECT 42 as z, 'bob' as bob"))
          (attr (pg-result res :attributes)))
     (should (string= "z" (caar attr)))
     (should (string= "bob" (caadr attr))))
   (unless (member (pgcon-server-variant con) '(cratedb risingwave))
     (let* ((res (pg-exec con "SELECT 32 as éléphant"))
            (attr (pg-result res :attributes)))
       (should (string= "éléphant" (caar attr)))
       (should (eql 32 (car (pg-result res :tuple 0))))))
   ;; Test PREPARE / EXECUTE
   (unless (member (pgcon-server-variant con) '(cratedb))
     (pg-exec con "PREPARE ps42 AS SELECT 42")
     (let ((res (pg-exec con "EXECUTE ps42")))
       (should (eql 42 (car (pg-result res :tuple 0)))))
     (pg-exec con "DEALLOCATE ps42"))
   (unless (member (pgcon-server-variant con) '(xata materialize))
     (let ((res (pg-exec con "EXPLAIN ANALYZE SELECT 42")))
       ;; CrateDB returns "EXPLAIN 1". The output from EXPLAIN ANALYZE is returned as a hash table.
       (unless (member (pgcon-server-variant con) '(cratedb))
         (should (string-prefix-p "EXPLAIN" (pg-result res :status)))
         (should (cl-every (lambda (r) (stringp (car r))) (pg-result res :tuples))))))
   ;; check query with empty column list
   (unless (member (pgcon-server-variant con) '(cratedb))
     (let ((res (pg-exec con "SELECT FROM information_schema.routines")))
       (should (eql nil (pg-result res :attributes)))
       (should (cl-every #'null (pg-result res :tuples))))))


(defun pg-test-cursors (con)
  (when (member "cursor_test" (pg-tables con))
    (pg-exec con "DROP TABLE cursor_test"))
  (let ((res (pg-exec con "BEGIN")))
    (should (string= "BEGIN" (pg-result res :status))))
  (pg-exec con (pgtest-massage con "CREATE TABLE cursor_test (a INTEGER PRIMARY KEY, b TEXT)"))
  (dotimes (i 10)
    (pg-exec con (format "INSERT INTO cursor_test VALUES(%d, '%d')" i i)))
  (let ((res (pg-exec con "DECLARE crsr42 CURSOR FOR SELECT * FROM cursor_test WHERE a=2")))
    (should (string= "DECLARE CURSOR" (pg-result res :status))))
  (let ((res (pg-exec con "FETCH 1000 FROM crsr42")))
    (should (string= "FETCH 1" (pg-result res :status)))
    (should (eql 1 (length (pg-result res :tuples)))))
  (let ((res (pg-exec con "FETCH 10 FROM crsr42")))
    (should (eql 0 (length (pg-result res :tuples)))))
  (let ((res (pg-exec con "CLOSE crsr42")))
    (should (string= "CLOSE CURSOR" (pg-result res :status))))
  (let ((res (pg-exec con "COMMIT")))
    (should (string= "COMMIT" (pg-result res :status))))
  (pg-exec con "DROP TABLE cursor_test"))


(defun pg-test-createdb (con)
  (when (member "pgeltestextra" (pg-databases con))
    (pg-exec con "DROP DATABASE pgeltestextra"))
  (pg-exec con "CREATE DATABASE pgeltestextra")
  (should (member "pgeltestextra" (pg-databases con)))
  ;; CockroachDB and YugabyteDB don't implement REINDEX. Also, REINDEX at the database level is
  ;; disabled on certain installations (e.g. Supabase), so we check reindexing of a table.
  (unless (member (pgcon-server-variant con) '(cockroachdb yugabyte))
    (pg-exec con "DROP TABLE IF EXISTS foobles")
    (pg-exec con (pgtest-massage con "CREATE TABLE foobles(a INTEGER PRIMARY KEY, b TEXT)"))
    (pg-exec con "CREATE INDEX idx_foobles ON foobles(a)")
    (pg-exec con "INSERT INTO foobles VALUES (42, 'foo')")
    (pg-exec con "INSERT INTO foobles VALUES (66, 'bizzle')")
    (unless (member (pgcon-server-variant con) '(risingwave materialize cedardb))
      (pg-exec con "REINDEX INDEX idx_foobles"))
    (when (and (> (pgcon-server-version-major con) 11)
               (not (member (pgcon-server-variant con) '(risingwave greenplum orioledb cedardb))))
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
  (pg-exec con "CREATE DATABASE pgel😎 WITH ENCODING 'UTF8'")
  (should (member "pgel😎" (pg-databases con)))
  (pg-exec con "DROP DATABASE pgel😎")
  (pg-exec con "CREATE TEMPORARY TABLE pgel😏(data TEXT)")
  (pg-exec con "INSERT INTO pgel😏 VALUES('Foobles')")
  (let ((r (pg-exec con "SELECT * FROM pgel😏")))
    (should (eql 1 (length (pg-result r :tuples)))))
  (pg-exec-prepared con "CREATE SCHEMA IF NOT EXISTS un␂icode" nil)
  (pg-exec-prepared con
                    (pgtest-massage con "CREATE TABLE IF NOT EXISTS un␂icode.ma🪄c(data TEXT PRIMARY KEY)")
                    nil)
  (pg-exec-prepared con "INSERT INTO un␂icode.ma🪄c VALUES($1)" '(("hi" . "text")))
  (let ((r (pg-exec con "SELECT * FROM un␂icode.ma🪄c")))
    (should (eql 1 (length (pg-result r :tuples)))))
  (pg-exec con "DROP TABLE un␂icode.ma🪄c")
  (pg-exec con "DROP SCHEMA un␂icode")
  (pg-exec con (pgtest-massage con "CREATE TEMPORARY TABLE pgeltestunicode(pg→el TEXT PRIMARY KEY)"))
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
  (when (pgtest-have-table con "pgeltestr")
    (pg-exec con "DROP TABLE pgeltestr"))
  (pg-exec con (pgtest-massage con "CREATE TABLE pgeltestr(id INTEGER NOT NULL PRIMARY KEY, data TEXT)"))
  (let* ((res (pg-exec con "INSERT INTO pgeltestr VALUES (1, 'Foobles') RETURNING id"))
         (id (cl-first (pg-result res :tuple 0)))
         (_ (pgtest-flush-table con "pgeltestr"))
         (res (pg-exec con (format "SELECT data from pgeltestr WHERE id=%s" id))))
    (should (string= (car (pg-result res :tuple 0)) "Foobles")))
  (pg-exec con "DROP TABLE pgeltestr"))


;; Test our support for handling ParameterStatus messages, via the pg-parameter-change-functions
;; variable. When we change the session timezone, the backend should send us a ParameterStatus
;; message with TimeZone=<new-value>.
(defun pg-test-parameter-change-handlers (con)
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
    (should-error (scalar "SELECT 2147483649::int4"))
    (should (eql -42 (scalar "SELECT -42")))
    (should-error (scalar "SELECT 'foobles'::unexistingtype"))
    (should (eql -55 (scalar "SELECT -55")))))

;; Here we test that the SQLSTATE component of errors signaled by the backend is valid.
;;
;; Some examples from https://github.com/technicaldeft/rgsql
(defun pg-test-error-sqlstate-helper (con scalar-fn)
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 42/0")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1/0::float8")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1/0::int2")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1/0::numeric")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1::numeric/0")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1::int8/0")
                     (pg-division-by-zero 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT sqrt(-5.0)")
                     (pg-floating-point-exception 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT log(-2.1)")
                     (pg-floating-point-exception 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT asin(2.0)")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT abs(true)")
                     (pg-undefined-function 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1 + true")
                     (pg-undefined-function 'ok)
                     ;; CockroachDB reports this as a pg-data-error.
                     (pg-data-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 'foo' + 'a'::character")
                     (pg-undefined-function 'ok)
                     ;; CockroachDB reports this as a pg-data-error.
                     (pg-data-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '25:61:61'::time")
                     (pg-data-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT ''::char(0)")
                     (pg-data-error 'ok)
                     ;; CockroachDB reports this as a syntax error
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT E'\\777'::bytea")
                     (pg-data-error 'ok)
                     ;; CockroachDB reports this as a syntax error
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; numerical overflow
                       (funcall scalar-fn "SELECT 2147483649::int4")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; numerical overflow on smallint
                       (funcall scalar-fn "SELECT (-32768)::int2 * (-1)::int2")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; numerical overflow on smallint
                       (funcall scalar-fn "SELECT (-32768)::int2 / (-1)::int2")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT  -2147483647::integer - 2::integer")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT -12345::numeric(3)")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 2345.678::numeric(5,2)")
                     (pg-numeric-value-out-of-range 'ok))))
  ;; Yugabyte doesn't accept this input syntax for smallint, nor PostgreSQL versions < 16
  (unless (or (member (pgcon-server-variant con) '(yugabyte greenplum))
              (< (pgcon-server-version-major con) 16))
    (should (eql 'ok (condition-case nil
                         ;; numerical overflow on smallint
                         (funcall scalar-fn "SELECT int2 '-0b1000000000000001'")
                       (pg-numeric-value-out-of-range 'ok)))))
  (should (eql 'ok (condition-case nil
                       ;; Can't OR bitstrings of different length
                       (funcall scalar-fn "SELECT B'10001' | B'001'")
                     (pg-data-error 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; numerical overflow
                       (funcall scalar-fn "SELECT lcm((-9223372036854775808)::int8, 1::int8)")
                     (pg-numeric-value-out-of-range 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; numerical overflow
                       (funcall scalar-fn "SELECT '10e-400'::float8")
                     (pg-numeric-value-out-of-range 'ok))))
  ;; xata.io fails on this test; returns a generic pg-error.
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT happiness(42)")
                     (pg-undefined-function 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECTING 42")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 1 * / 2")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT abs 1)")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT mod 1, 2")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 42 as 42a")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT @, $, %, &")
                     (pg-syntax-error 'ok))))
  ;; Reserved keyword used as table name.
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE from(a INTEGER)")
                     (pg-syntax-error 'ok))))
  ;; Reserved keyword used as column name.
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE x(as INTEGER)")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '[1,2,3]'::json ->> {}")
                     (pg-syntax-error 'ok))))
  (when (> (pgcon-server-version-major con) 11)
    (unless (member (pgcon-server-variant con) '(cockroachdb))
      (should (eql 'ok (condition-case nil
                           (funcall scalar-fn "SELECT jsonb_path_query('{\"a\":42}'::jsonb, '$$.foo')")
                         (pg-syntax-error 'ok))))))
  ;; The json_serialize() function is new in PostgreSQL 17
  (unless (or (member (pgcon-server-variant con) '(cockroachdb yugabyte alloydb))
              (< (pgcon-server-version-major con) 17))
    (should (eql 'ok (condition-case nil
                         (funcall scalar-fn "SELECT json_serialize('{\"a\": \"foo\", 42: 43 }')")
                       (pg-invalid-text-representation 'ok))))
    (should (eql 'ok (condition-case nil
                         ;; cannot use non-string types with implicit FORMAT JSON clause
                         (funcall scalar-fn "SELECT JSON_SERIALIZE(144)")
                       (pg-datatype-mismatch 'ok)))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT CAST('55Y' AS INTEGER)")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 'not-a-uuid'::uuid")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT * FROM nonexistent_table")
                     (pg-undefined-table 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE foobles(a BANANA)")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER TABLE nonexistent_table RENAME TO aspirational")
                     (pg-undefined-table 'ok))))
  (funcall scalar-fn "CREATE TABLE pgtest_foobles(a INTEGER PRIMARY KEY)")
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER TABLE pgtest_foobles DROP COLUMN nonexistent")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER TABLE pgtest_foobles RENAME COLUMN nonexistent TO aspirational")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER TABLE pgtest_foobles RENAME TO pgtest_foobles")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       ;; This creates a conflict with the system column ctid
                       (funcall scalar-fn "ALTER TABLE pgtest_foobles RENAME COLUMN a TO ctid")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE INDEX pgtest_idx ON pgtest_foobles(inexist)")
                     (pg-programming-error 'ok))))
  (funcall scalar-fn "DROP TABLE pgtest_foobles")
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "DROP INDEX nonexist_idx")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "DROP VIEW nonexist_view")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "DROP TYPE")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "DROP TYPE nonexist_type")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "DEALLOCATE nonexistent_prepared_statement")
                     (pg-invalid-sql-statement-name 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CLOSE nonexistent_cursor")
                     (pg-invalid-cursor-name 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "EXECUTE phantom_prepared_statement")
                     (pg-invalid-sql-statement-name 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER DATABASE pgeltest_nonexistent REFRESH COLLATION VERSION")
                     (pg-invalid-catalog-name 'ok))))
  ;; In PostgreSQL 18.1, this does not trigger an error.
  (should (eql nil (pg-close-portal con "nonexistent_portal")))
  ;; CockroachDB reports this as a pg-data-error, and PostgreSQL as pg-undefined-function.
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 100 = false")
                     (pg-undefined-function 'ok)
                     (pg-data-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT banana(42)")
                     (pg-undefined-function 'ok))))
  ;; insufficient arguments means operator does not exist
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT mod(42)")
                     (pg-undefined-function 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT unexist FROM pg_catalog.pg_type")
                     (pg-undefined-column 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECTING incorrect-syntax")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT * FRÖM VALUES(1,2)")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '[1,2,3]'::json ->> gg")
                     (pg-undefined-column 'ok))))
  ;; The ? operator is only defined for jsonb ? text
  (unless (member (pgcon-server-variant con) '(cockroachdb))
    (should (eql 'ok (condition-case nil
                         (funcall scalar-fn "SELECT '{\"a\":1, \"b\":2}'::jsonb ? 52")
                       (pg-undefined-function 'ok))))
    (should (eql 'ok (condition-case nil
                         (pg-exec-prepared con "SELECT $1[5]" '(("[1,2,3]" . "json")))
                       (pg-datatype-mismatch 'ok)))))
  (when (pg-function-p con "jsonb_path_query")
    (should (eql 'ok (condition-case nil
                         (funcall scalar-fn "SELECT jsonb_path_query('{\"h\": 1.7}', '$.floor()')")
                       (pg-json-error 'ok)))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '{1:\"abc\"}'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '{\"abc\":1:2}'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '{\"abc\":1,3}'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '1f2'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT int2 '10__000'")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '{ }}'::text[]")
                     (pg-invalid-text-representation 'ok)
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 'true false::json")
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT 'true false'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '    '::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '\"\\u00\"'::json")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '\"\\u\"'::jsonb")
                     (pg-invalid-text-representation 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT E'\\xDEADBEEF'")
                     ;; "Invalid byte sequence for encoding"
                     (pg-character-not-in-repertoire 'ok)
                     ;; CockroachDB reports this as a syntax error (different SQLSTATE value)
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "SELECT '2024-15-01'::date")
                     (pg-datetime-field-overflow 'ok))))
  (should (eql 'ok (condition-case nil
                       (progn
                         (funcall scalar-fn "DEALLOCATE ALL")
                         (funcall scalar-fn "PREPARE pgeltestq1(text, int, float, boolean, smallint) AS SELECT 42")
                         ;; too many params
                         (funcall scalar-fn "EXECUTE pgeltestq1('AAAAxx', 5::smallint, 10.5::float, false, 4::bigint, 15::int2)"))
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "PREPARE pgeltestq1(text, int, float, boolean, smallint) AS SELECT 42")
                     (pg-duplicate-prepared-statement 'ok))))
  (should (eql 'ok (condition-case nil
                       (progn
                         (funcall scalar-fn "CREATE TABLE test_duplicate(a INTEGER)")
                         (funcall scalar-fn "CREATE TABLE test_duplicate(a INTEGER)"))
                     (pg-duplicate-table 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE duplicate_column(a INTEGER, a INTEGER)")
                     (pg-duplicate-column 'ok))))
  (should (eql 'ok (condition-case nil
                       (progn
                         (funcall scalar-fn "PREPARE pgeltestq2(text, int, float, boolean, smallint) AS SELECT 42")
                         ;; too few params
                         (funcall scalar-fn "EXECUTE pgeltestq2('bool')"))
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (progn
                         (funcall scalar-fn "PREPARE pgeltestq3(text, int, float, boolean, smallint) AS SELECT 42")
                         ;; wrong parameter types
                         (funcall scalar-fn "EXECUTE pgeltestq3(5::smallint, 10.5::float, false, 4::bigint, 'bytea')"))
                     (pg-datatype-mismatch 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER OPERATOR @+@(int4, int4) OWNER TO pgeltest_notexist")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "ALTER OPERATOR @+@(int4, int4) SET SCHEMA pgeltest_notexist")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE pgtest_dupcol(a INTEGER PRIMARY KEY, a VARCHAR)")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE -----(a INTEGER PRIMARY KEY)")
                     (pg-programming-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE table(a INTEGER PRIMARY KEY)")
                     (pg-reserved-name 'ok)
                     (pg-syntax-error 'ok))))
  (should (eql 'ok (condition-case nil
                       (funcall scalar-fn "CREATE TABLE foo (LIKE nonexistent)")
                     (pg-programming-error 'ok))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "CREATE TABLE pgtest_notnull(a INTEGER NOT NULL PRIMARY KEY)")
                     (funcall scalar-fn "INSERT INTO pgtest_notnull(a) VALUES (6)")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_notnull(a) VALUES (NULL)")
                       (pg-not-null-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_notnull"))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "CREATE TABLE pgtest_unique(a INTEGER PRIMARY KEY)")
                     (funcall scalar-fn "INSERT INTO pgtest_unique(a) VALUES (6)")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_unique(a) VALUES (6)")
                       (pg-unique-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_unique"))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "CREATE TABLE pgtest_check(a INTEGER PRIMARY KEY CHECK (a > 0))")
                     (funcall scalar-fn "INSERT INTO pgtest_check(a) VALUES (6)")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_check(a) VALUES (-2)")
                       (pg-check-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_check"))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "CREATE TABLE pgtest_check_num(a INTEGER PRIMARY KEY, b NUMERIC NOT NULL CHECK (b > 10))")
                     (funcall scalar-fn "INSERT INTO pgtest_check_num(a,b) VALUES(1, 15.0)")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_check_num(a,b) VALUES(2, 5.0)")
                       (pg-check-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_check_num"))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "CREATE TABLE pgtest_check_in(id INTEGER PRIMARY KEY, status TEXT CHECK (status IN ('OK', 'NOK')))")
                     (funcall scalar-fn "INSERT INTO pgtest_check_in(id,status) VALUES(1, 'OK')")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_check_in(id,status) VALUES(2, 'INVALID')")
                       (pg-check-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_check_in"))))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "DROP DOMAIN IF EXISTS hex_number")
                     (funcall scalar-fn "CREATE DOMAIN hex_number AS TEXT CHECK (VALUE ~* '^[0-9A-Fa-f]+$')")
                     (funcall scalar-fn "CREATE TABLE pgtest_check_domain(hex hex_number NOT NULL)")
                     (funcall scalar-fn "INSERT INTO pgtest_check_domain VALUES('FF11')")
                     (condition-case nil
                         (funcall scalar-fn "INSERT INTO pgtest_check_domain VALUES('INVALID')")
                       (pg-check-violation 'ok)))
                 (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_check_domain")
                 (funcall scalar-fn "DROP DOMAIN hex_number"))))
  ;; As of 2025-02, yugabyte, CockroachDB and OrioleDB do not implement EXCLUDE constraints.
  (unless (member (pgcon-server-variant con) '(yugabyte cockroachdb orioledb))
    (should (eql 'ok
                 (unwind-protect
                     (progn
                       (funcall scalar-fn "CREATE TABLE pgtest_exclude(a INTEGER, EXCLUDE (a WITH =))")
                       (funcall scalar-fn "INSERT INTO pgtest_exclude(a) VALUES (6)")
                       (condition-case nil
                           (funcall scalar-fn "INSERT INTO pgtest_exclude(a) VALUES (6)")
                         (pg-exclusion-violation 'ok)))
                   (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_exclude")))))
  ;; Greenplum does not implement FOREIGN KEY integrity constraints
  (unless (member (pgcon-server-variant con) '(greenplum))
    (unwind-protect
        (progn
          (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_referencing")
          (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_referenced")
          (funcall scalar-fn "CREATE TABLE pgtest_referenced(a INTEGER PRIMARY KEY)")
          (funcall scalar-fn "CREATE TABLE pgtest_referencing(a INTEGER NOT NULL REFERENCES pgtest_referenced(a))")
          (funcall scalar-fn "INSERT INTO pgtest_referenced(a) VALUES (6)")
          (funcall scalar-fn "INSERT INTO pgtest_referencing(a) VALUES (6)")
          (should (eql 'ok
                       (condition-case nil
                           (funcall scalar-fn "INSERT INTO pgtest_referencing(a) VALUES (1)")
                         (pg-foreign-key-violation 'ok)))))
      (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_referencing")
      (funcall scalar-fn "DROP TABLE IF EXISTS pgtest_referenced")))
  (should (eql 'ok
               (unwind-protect
                   (progn
                     (funcall scalar-fn "SET default_transaction_read_only TO on")
                     (funcall scalar-fn "BEGIN")
                     (condition-case nil
                         (funcall scalar-fn "CREATE TABLE erroring(id SERIAL)")
                       (pg-error 'ok)))
                 (funcall scalar-fn "END")
                 (funcall scalar-fn "SET default_transaction_read_only TO off"))))
  ;; handler-bind is new in Emacs 30. Here we check the printed representation of our
  ;; pg-undefined-function error class.
  (when (fboundp 'handler-bind)
    (should (eql 'ok
                 (catch 'pgtest-undefined-function
                   (handler-bind
                       ((pg-undefined-function
                         (lambda (e)
                           (should (cl-search "undef" (prin1-to-string e)))
                           (throw 'pgtest-undefined-function 'ok)))
                        (pg-error
                         (lambda (_e)
                           (error "Unexpected error class"))))
                     (funcall scalar-fn "SELECT undef(42)"))
                   'nok))))
  ;;     (should (eql 'ok (condition-case nil
  ;;                          (pg-exec-prepared con "SELECT $1[-5]" '(("{1,2,3}" . "_int4")))
  ;;                        (pg-syntax-error 'ok))))
  )

;; We test first for correct SQLSTATE handling with the simple query protocol, then with the
;; extended query protocol.
(defun pg-test-error-sqlstate (con)
  (pg-test-error-sqlstate-helper con 
                                 (lambda (sql) (car (pg-result (pg-exec con sql) :tuple 0))))
  (pg-test-error-sqlstate-helper con
                                 (lambda (sql) (car (pg-result (pg-exec-prepared con sql nil) :tuple 0)))))


;; Check our handling of NoticeMessage messages, and the correct operation of
;; `pg-handle-notice-functions'.
(defun pg-test-notice (con)
  ;; The DROP TABLE will generate a NOTICE. We install a handler function that checks for the
  ;; name of the table in the NOTICE message (the message will be localized, but hopefully the
  ;; table name will always be present).
  (cl-flet ((deity-p (ntc) (should (cl-search "deity" (pgerror-message ntc)))))
    (let ((pg-handle-notice-functions (list #'deity-p)))
      (pg-exec con "DROP TABLE IF EXISTS deity")))
  ;; CrateDB does not support ROLLBACK
  (unless (member (pgcon-server-variant con) '(cratedb))
    (cl-flet ((check-user-abort (ntc)
                (should (string= "25P01" (pgerror-sqlstate ntc)))
                (should (string= "UserAbortTransactionBlock" (pgerror-routine ntc)))))
      (let ((pg-handle-notice-functions (list #'check-user-abort)))
        (pg-exec con "ROLLBACK"))))
  ;; CrateDB and Spanner do not support DO. GreptimeDB does not support SET client_min_messages.
  (unless (member (pgcon-server-variant con) '(cratedb spanner greptimedb questdb))
    (cl-flet ((check-shibboleth (ntc)
                (should (cl-search "Shibboleτℋ" (pgerror-message ntc)))))
      (let ((pg-handle-notice-functions (list #'check-shibboleth)))
        (pg-exec con "SET client_min_messages TO notice")
        (pg-exec con "DO $$BEGIN raise notice 'Hi! Shibboleτℋ'; END$$ LANGUAGE PLPGSQL")))
    (cl-flet ((check-shibboleth (ntc)
                (should (cl-search "ShibboleTH" (pgerror-message ntc)))
                (should (string= "WARNING" (pgerror-severity ntc)))))
      (let ((pg-handle-notice-functions (list #'check-shibboleth)))
        (pg-exec con "SET client_min_messages TO warning")
        (pg-exec con "DO $$BEGIN raise notice 'Intruder here'; END$$ LANGUAGE PLPGSQL")
        (pg-exec con "DO $$BEGIN raise warning 'Hi! ShibboleTH'; END$$ LANGUAGE PLPGSQL")))))

;; Check handling of asynchronous notifications, as generated by LISTEN/NOTIFY. Note that this test
;; is not actually relying on any asynchronous functionality; the notification is received in
;; response to the dummy SELECT request.
(defun pg-test-notify (con)
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
    (unless (member (pgcon-server-variant con) '(xata))
      (pg-exec con "SELECT pg_notify('yourheart', 'leaving')"))
    (pg-exec con "SELECT 'ignored'")
    (pg-exec con "UNLISTEN yourheart")
    (should-error
     (pg-exec con "UNLISTEN invalid-value")
     :type 'pg-syntax-error)
    (should-error
     (pg-exec con "LISTEN 42")
     :type 'pg-syntax-error)
    (pg-exec con "NOTIFY yourheart, 'Et redit in nihilum quod fuit ante nihil.'")))


;; Only the superuser can issue a VACUUM. A bunch of NOTICEs will be emitted indicating this. This
;; test is not robust across PostgreSQL versions, however.
; (let ((notice-counter 0))
;   (let ((pg-handle-notice-functions (list (lambda (_n) (cl-incf notice-counter)))))
;     (pg-exec con "VACUUM")
;     (should (> notice-counter 0)))))


(defun pg-test-lo (con)
  (pg-test-lo-read con)
  (pg-test-lo-ensure-size con)
  (pg-test-lo-import con)
  (pg-test-lo-errors con)
  (pg-exec con "SELECT 42"))

;; Note the use of with-pg-transaction to wrap the requests in a BEGIN..END transaction which is
;; necessary when working with large objects.
(defun pg-test-lo-read (con)
  (message "Testing lo-read and friends")
  (with-pg-transaction con
    (let* ((oid (pg-lo-create con "rw"))
           (fd (pg-lo-open con oid "rw")))
      (unless (member (pgcon-server-variant con) '(yugabyte))
        (let* ((sql "SELECT oid FROM pg_catalog.pg_largeobject_metadata WHERE oid=$1")
               (res (pg-exec-prepared con sql `((,oid . "int4"))))
               (rows (pg-result res :tuples)))
          (should (eql 1 (length rows)))))
      (pg-lo-write con fd "Hi there mate")
      (pg-lo-lseek con fd 3 pg-SEEK_SET)
      (should (eql 3 (pg-lo-tell con fd)))
      (let ((substring (pg-lo-read con fd 7)))
        (should (string= substring "there m")))
      ;; Test the server-side function lo_get(), as per
      ;; https://www.postgresql.org/docs/current/lo-funcs.html We don't have to decode this value
      ;; from hex because the prepared statement infrastructure in pg-el does that for us.
      (let ((res (pg-exec-prepared con "SELECT lo_get($1, 9, 4)" `((,oid . "int4")))))
        (should (string= "mate" (cl-first (pg-result res :tuple 0)))))
      (pg-lo-close con fd)
      (pg-lo-unlink con oid))))

(defun pg-test-lo-ensure-size (con)
  (message "Testing lo-lseek and friends")
  (with-pg-transaction con
    (let* ((oid (pg-lo-create con "rw"))
           (fd (pg-lo-open con oid "rw"))
           (filler (make-string (* 1024 1024) ?Z))
           (target-len (* 1024 1024 1024)))
      (dotimes (i 512)
        (pg-exec-prepared con "SELECT lo_put($1, $2, $3)"
                          `((,oid . "int4") (,(* i 1024 1024) . "int8") (,filler . "bytea"))))
      (pg-lo-lseek con fd (* 512 1024 1024) pg-SEEK_SET)
      (dotimes (_ 512)
        (should (eql (* 1024 1024) (pg-lo-write con fd filler))))
      ;; Now check that the octets have been written as expected
      (let ((pos (pg-lo-lseek con fd 0 pg-SEEK_CUR)))
        (should (eql pos target-len)))
      (let ((pos (pg-lo-tell con fd)))
        (should (eql pos target-len)))
      (let ((pos (pg-lo-lseek con fd 0 pg-SEEK_END)))
        (should (eql pos target-len)))
      (dotimes (_ 100)
        (let ((pos (random target-len)))
          (pg-lo-lseek con fd pos pg-SEEK_SET)
          (should (string= "Z" (pg-lo-read con fd 1)))))
      (dotimes (_ 100)
        (let* ((count (random 20))
               (pos (max (- target-len count 1) (random target-len)))
               (res (pg-exec-prepared con "SELECT lo_get($1, $2, $3)"
                                      `((,oid . "int4") (,pos . "int8") (,count . "int4"))))
               (actual (cl-first (pg-result res :tuple 0))))
          (should (string= (make-string count ?Z) actual))))
      (let* ((halfway (* 512 1024 1024)))
        (pg-lo-truncate con fd halfway)
        (should (eql halfway (pg-lo-lseek con fd 0 pg-SEEK_END)))
        (pg-lo-lseek con fd 0 pg-SEEK_SET)
        (let ((filler (make-string (* 1024 1024) ?#)))
          (dotimes (i 512)
            (pg-lo-write con fd filler)))
        (dotimes (_ 100)
          (let ((pos (random halfway)))
            (pg-lo-lseek con fd pos pg-SEEK_SET)
            (should (string= "#" (pg-lo-read con fd 1))))))
      (pg-lo-close con fd)
      (pg-lo-unlink con oid))))

(defun pg-test-lo-import (con)
  (message "Testing lo-import and friends")
  (with-pg-transaction con
    (let ((oid (pg-lo-import con "/etc/group")))
      (unless (member (pgcon-server-variant con) '(yugabyte))
        (let* ((sql "SELECT oid FROM pg_catalog.pg_largeobject_metadata WHERE oid=$1")
               (res (pg-exec-prepared con sql `((,oid . "int4"))))
               (rows (pg-result res :tuples)))
          (should (eql 1 (length rows)))
          (should (eql oid (caar rows)))))
      (pg-lo-export con oid "/tmp/group")
      (cond ((zerop (call-process "diff" nil nil nil "/tmp/group" "/etc/group"))
             (message "lo-import test succeeded")
             (delete-file "/tmp/group"))
            (t
             (message "lo-import test failed: check differences")
             (message "between files /etc/group and /tmp/group")))
      (pg-lo-unlink con oid))))

;; This test is not run within a transaction, because each error aborts the current transaction.
(defun pg-test-lo-errors (con)
  (message "Testing error handling in lo functions")
  (cl-flet ((flush-pending ()
            (let ((res (pg-exec con "SELECT -5")))
              (should (eql -5 (cl-first (pg-result res :tuple 0)))))))
    ;; Test for a double close
    (with-pg-transaction con
       (let* ((oid (pg-lo-create con "rw"))
              (fd (pg-lo-open con oid "rw")))
         (pg-lo-close con fd)
         (should-error
          (pg-lo-close con fd)
          :type 'pg-programming-error)))
    (flush-pending)
    ;; Test for a double unlink
    (with-pg-transaction con
     (let* ((oid (pg-lo-create con "rw"))
            (fd (pg-lo-open con oid "rw")))
       (pg-lo-close con fd)
       (pg-lo-unlink con oid)
       (should-error
        (pg-lo-close con oid)
        :type 'pg-programming-error)))
    (flush-pending)
    (should-error
     (pg-lo-lseek con -1 0 (+ 10 pg-SEEK_SET)))
    (flush-pending)
    (should-error
     (pg-lo-close con 111)
     :type 'pg-programming-error)
    (flush-pending)
    (should-error
     (pg-lo-unlink con -6)
     :type 'pg-user-error)
    (flush-pending)
    (should-error
     (pg-lo-read con 0 -10))
    (flush-pending)
    (should-error
     (pg-lo-read con -106 10))
    (flush-pending)
    (should-error
     (pg-lo-read nil 10 10))
    (flush-pending)
    (should-error
     (pg-lo-truncate 42 42))
    (flush-pending)
    (should-error
     (pg-lo-lseek con 42 42 pg-SEEK_END))
    (flush-pending)))


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

(defmacro pg-assert-string= (expected test-form)
  `(unless (string= ,expected ,test-form)
     (error "Test failure: %s => %s (expecting %s)"
            ',test-form ,test-form ,expected)))

(defun pg-run-tz-tests (con)
  (pg-exec con "DROP TABLE IF EXISTS tz_test")
  (pg-exec con (pgtest-massage con "CREATE TABLE tz_test(id INTEGER PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ)"))
  ;; This is the same as CET: in a Posix time zone specification, a positive sign is used for zones
  ;; west of Greenwich, which is the opposite (!) of the ISO-8601 sign convention used when printing
  ;; timestamps. However, Emacs on certain platforms like Windows has a very limited ability to
  ;; interpret timezones like Europe/Paris or CET, so we use this format instead.
  (with-environment-variables (("TZ" "UTC-01:00"))
    (pg-exec con "SET TimeZone = 'UTC-01:00'")
    (unwind-protect
        (progn
          (pg-test-iso8601-regexp)
          (when (version<= "29.1" emacs-version)
            (pg-test-parse-ts con))
          (pg-test-serialize-ts con)
          (unless (member (pgcon-server-variant con) '(cratedb))
            ;; CrateDB is returning "1709033682789" instead of "2024-02-27 11:34:42.789" for ts::text
            (pg-test-insert-literal-ts con)
            (when (version<= "29.1" emacs-version)
              (pg-test-insert-parsed-ts con))))
      (pg-exec con "DROP TABLE IF EXISTS tz_test"))))

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

(defun pg-test-parse-ts (_con)
  (message "Test parsing of timestamps ...")
  (let ((ts (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789+04" nil))
        (ts-dst (pg-isodate-without-timezone-parser "2024-05-27T11:34:42.789+04" nil))
        (ts-no-tz (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789" nil))
        (ts-zulu (pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789Z" nil))
        (tstz (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789+04" nil))
        (tstz-dst (pg-isodate-with-timezone-parser "2024-05-27T15:34:42.789+04" nil))
        (tstz-no-tz (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789" nil))
        (tstz-zulu (pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789Z" nil)))
    ;; This function is being run with TZ=UTC-1. There is a one hour difference between UTC and
    ;; UTC-1. If we were using a location-based timezone such as Europe/Paris, there would be a 1
    ;; hour difference with UTC in February, and a 2 hour difference in May. We don't test this
    ;; because the GitHub actions runners are not all able to parse a Europe/Paris timezone.
    (should (string= "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts)))
    (pg-assert-string= "2024-05-27T10:34:42.789+0000" (pg-fmt-ts-utc ts-dst))
    (pg-assert-string= "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts-no-tz))
    (pg-assert-string= "2024-02-27T10:34:42.789+0000" (pg-fmt-ts-utc ts-zulu))
    (pg-assert-string= "2024-02-27T11:34:42.789+0000" (pg-fmt-ts-utc tstz))
    (pg-assert-string= "2024-05-27T11:34:42.789+0000" (pg-fmt-ts-utc tstz-dst))
    (pg-assert-string= "2024-02-27T14:34:42.789+0000" (pg-fmt-ts-utc tstz-no-tz))
    (pg-assert-string= "2024-02-27T15:34:42.789+0000" (pg-fmt-ts-utc tstz-zulu))))

(defun pg-test-serialize-ts (_con)
  (message "Test serialization of timestamps ...")
  (let* ((ts (encode-time (iso8601-parse "2024-02-27T15:34:42.789+04" t)))
         (ts-ser (pg--serialize-encoded-time ts nil)))
    (pg-assert-string= "2024-02-27T11:34:42.789000000+0000" ts-ser)))

;; The timestamp with timezone type is converted to UTC on input and stored without any timezone
;; information. On output (during the cast to text) the timezone is converted and printed in the
;; current session's timezone.
;;
;; https://www.postgresql.org/docs/current/datatype-datetime.html
;; https://www.postgresql.org/docs/14//datetime-posix-timezone-specs.html
(defun pg-test-insert-literal-ts (con)
  (message "Test literal (string) timestamp insertion ...")
  ;; We take this as reference. It behaves exactly like psql.
  ;; Entering literals works as expected. Note that we cast to text to rule out deserialization errors.
  (pg-exec con "SET TimeZone = 'Etc/UTC'")
  (pg-exec con "INSERT INTO tz_test(id, ts, tstz) VALUES(1, '2024-02-27T11:34:42.789+04', '2024-02-27T15:34:42.789+04')")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=1") :tuple 0))
         (ts (nth 0 data))
         (tstz (nth 1 data)))
    ;; Here we are assuming that DateStyle=ISO (this is the default setting)
    (pg-assert-string= "2024-02-27 11:34:42.789" ts)
    (pg-assert-string= "2024-02-27 11:34:42.789+00" tstz))
  (pg-exec con "SET TimeZone = 'UTC-01:00'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=1") :tuple 0))
         (ts (nth 0 data))
         (tstz (nth 1 data)))
    (pg-assert-string= "2024-02-27 11:34:42.789" ts)
    (pg-assert-string= "2024-02-27 12:34:42.789+01" tstz)))

(defun pg-test-insert-parsed-ts (con)
  (message "Test object timestamp insertion ...")
  (pg-exec-prepared con "INSERT INTO tz_test(id, ts, tstz) VALUES(2, $1, $2)"
                    `((,(pg-isodate-without-timezone-parser "2024-02-27T11:34:42.789+04" nil) . "timestamp")
                      (,(pg-isodate-with-timezone-parser "2024-02-27T15:34:42.789+04:00" nil) . "timestamptz")))
  (pg-exec con "SET TimeZone = 'Etc/UTC'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=2") :tuple 0))
         (ts (nth 0 data))
         (tstz (nth 1 data)))
    (pg-assert-string= "2024-02-27 10:34:42.789" ts)
    (pg-assert-string= "2024-02-27 11:34:42.789+00" tstz))
  (pg-exec con "SET TimeZone = 'UTC-01:00'")
  (let* ((data (pg-result (pg-exec con "SELECT ts::text, tstz::text FROM tz_test WHERE id=2") :tuple 0))
         (tstz (nth 1 data)))
    (pg-assert-string= "2024-02-27 12:34:42.789+01" tstz)))


(defun pg-assert-matches (str regexp)
  (should (string-match regexp str)))

(defun pg-assert-does-not-match (str regexp)
  (should-not (string-match regexp str)))

(defun pg-fmt-ts-utc (ts)
  (let ((ft "%Y-%m-%dT%H:%M:%S.%3N%z"))
    ;; Last argument of t means "UTC"
    (format-time-string ft ts t)))


;; EOF
