;;; Async notification tests for the pg.el library   -*- coding: utf-8; lexical-binding: t; -*-
;;;
;;; This allows implementation of basic publish-subscribe functionality. Say "make pubsub" to run
;;; this test with one message publisher and three message subscribers.
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2023  Eric Marsden


(require 'cl-lib)
(require 'pg)
(require 'ert)

(defmacro with-pgtest-connection (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port)
                         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432))))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port t)
                         ,@body)))

(defmacro with-pgtest-connection-local (conn &rest body)
  (let* ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
         (user (or (getenv "PGEL_USER") "pgeltestuser"))
         (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
         (port (let ((p (getenv "PGEL_PORT"))) (if p (string-to-number p) 5432)))
         (path (or (getenv "PGEL_PATH") (format "/var/run/postgresql/.s.PGSQL.%s" port))))
    `(with-pg-connection-local ,conn (,path ,db ,user ,password)
                               ,@body)))


;; Here we are listening to two notification channels, named yourheart and counting. The counting
;; channel receives 1000 notifications (simply the notification number as a string).
;;
;; We check here that notification data is not processed twice, once via the async notification
;; handler, and once via the synchronous request processing, thanks to the dummy SELECT statements.
;; A synchronous SELECT statement will cause any unprocessed notification messages to be processed
;; synchronously.
(defun do-listener ()
  (with-pgtest-connection-tls con
    (let ((seen (make-hash-table :test #'equal))
          (notification-count 0))
      (cl-flet ((notification-handler (_channel payload)
                  (when (gethash payload seen)
                    (message "Duplicate notification %s" payload))
                  (puthash payload t seen)
                  (cl-incf notification-count)))
        (pg-add-notification-handler con #'notification-handler)
        (pg-enable-async-notification-handlers con)
        (pg-exec con "LISTEN yourheart")
        (sleep-for 1)
        (pg-exec con "LISTEN counting")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 5)
        (pg-exec con "SELECT 42")
        (sleep-for 60)
        (message "Subscriber %s has seen %d notifications"
                 (emacs-pid) (hash-table-count seen))))))

