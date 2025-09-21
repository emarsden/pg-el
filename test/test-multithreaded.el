;;; Multithreaded tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2024  Eric Marsden


(require 'cl-lib)
(require 'pg)
(require 'ert)

(defmacro with-pgtest-connection (con &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection ,con (,db ,user ,password ,host ,port)
                         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (con &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection ,con (,db ,user ,password ,host ,port t)
                         ,@body)))

(defmacro with-pgtest-connection-local (con &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
    `(with-pg-connection-local ,con (,path ,db ,user ,password)
                               ,@body)))


(cl-defun pgtest-worker (table &optional (iterations 100))
  (message "Starting pg worker %s" table)
  ;; or with-pgtest-connection-local
  (with-pgtest-connection con
     (dotimes (iter iterations)
       (message "pg worker %s iteration %d" table iter)
       (pg-exec con (format "DROP TABLE IF EXISTS %s" table))
       (pg-exec con (format "CREATE TABLE %s(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value INTEGER)"
                            table))
       (let ((start (+ 3000 (random 4000))))
         (dotimes (i 100)
           (pg-exec-prepared con (format "INSERT INTO %s(value) VALUES ($1)" table)
                             `((,(+ start i) . "int4"))))
         (let* ((res (pg-exec con (format "SELECT COUNT(*) FROM %s" table)))
                (count (cl-first (pg-result res :tuple 0))))
           (unless (eql count 100)
             (message "Row count failure on table %s" table)))
         (dotimes (i 100)
           (pg-exec-prepared con (format "DELETE FROM %s WHERE value=$1" table)
                             `((,(+ start i) . "int4"))))
         (let* ((res (pg-exec con (format "SELECT COUNT(*) FROM %s" table)))
                (count (cl-first (pg-result res :tuple 0))))
           (unless (eql count 0)
             (message "Row count failure on table %s" table)))
         (pg-exec con (format "DROP TABLE %s" table)))
       (thread-yield)))
  (message "pg worker thread %s finished" table))


(defun pgtest-multithreaded ()
  (let ((workers (list)))
    (dotimes (i 5)
      (push (make-thread (lambda () (pgtest-worker (format "pgtest_table_%d_%d" (emacs-pid) i)))
                         (format "pgel-%d" i))
            workers))
    (message "Worker threads created; sleeping")
    (sit-for 10)
    (cl-loop while (cl-some #'thread-live-p workers)
             do (thread-yield) (accept-process-output nil 1))))

