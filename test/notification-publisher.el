;;; Asynchronous notification tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;;
;;; This allows implementation of basic publish-subscribe functionality.
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2023-2025  Eric Marsden


(require 'cl-lib)
(require 'pg)
(require 'ert)

(defmacro with-pgtest-connection (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection-plist ,conn (,db ,user :password ,password :host ,host :port ,port)
                         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection-plist ,conn (,db ,user :password ,password :host ,host :port ,port :tls-options t)
                         ,@body)))

(defmacro with-pgtest-connection-local (conn &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
    `(with-pg-connection-local ,conn (,path ,db ,user ,password)
                               ,@body)))


(defun do-publisher ()
  (cl-flet ((notification-handler (channel payload)
              (message "PUB> Async notification on %s: %s" channel payload)))
    (with-pgtest-connection-tls con
       (pg-add-notification-handler con #'notification-handler)
       (pg-exec con "NOTIFY yourheart, 'bizzles'")
       (sleep-for 4)
       (pg-exec con "NOTIFY yourheart, 'bazzles'")
       (pg-exec con "SELECT pg_sleep(4)")
       (sleep-for 3)
       (pg-exec con "NOTIFY yourheart, 'fooble'")
       (sleep-for 2)
       (pg-exec con "NOTIFY yourheart")
       (sleep-for 2)
       (dotimes (i 1000)
         (pg-exec con (format "NOTIFY counting, '%d'" i)))
       (sleep-for 2))))

