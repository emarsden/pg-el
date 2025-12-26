;;; pg.el --- Socket-level interface to the PostgreSQL database  -*- lexical-binding: t -*-

;; Copyright: (C) 1999-2002, 2022-2025  Eric Marsden

;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; Version: 0.62
;; Keywords: data comm database postgresql
;; URL: https://github.com/emarsden/pg-el
;; Package-Requires: ((emacs "28.1") (peg "1.0.1"))
;; SPDX-License-Identifier: GPL-3.0-or-later
;;
;; This file is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; This file is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this file.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Overview
;; --------
;;
;; This module lets you access the PostgreSQL database from Emacs, using its socket-level
;; frontend/backend protocol (the PostgreSQL wire protocol). The module is capable of automatic type
;; coercions from a range of SQL types to the equivalent Emacs Lisp type.
;;
;; Supported features:
;;
;;  - SCRAM-SHA-256 authentication (the default method since PostgreSQL version 14) and MD5
;;    authentication.
;;
;;  - Encrypted (TLS) connections between Emacs and the PostgreSQL backend.
;;
;;  - Parameterized queries using PostgreSQL's extended query syntax, to protect from SQL
;;    injection issues.
;;
;;  - The PostgreSQL COPY protocol to copy preformatted data to PostgreSQL from an Emacs
;;    buffer.
;;
;;  - Asynchronous handling of LISTEN/NOTIFY notification messages from PostgreSQL, allowing the
;;    implementation of publish-subscribe type architectures (PostgreSQL as an "event broker" or
;;    "message bus" and Emacs as event publisher and consumer).
;;
;;
;; This is a low level API, and won't be useful to end users. If you're looking for an
;; Emacs-based browsing/editing interface to PostgreSQL, see the PGmacs library at
;; https://github.com/emarsden/pgmacs/.
;;
;;
;; Entry points
;; ------------
;;
;; See the online documentation at <https://emarsden.github.io/pg-el/API.html>.

;; Thanks to Eric Ludlam for discovering a bug in the date parsing routines, to
;; Hartmut Pilch and Yoshio Katayama for adding multibyte support, and to Doug
;; McNaught and Pavel Janik for bug fixes.


;;; TODO
;;
;; * Implement the SASLPREP algorithm for usernames and passwords that contain
;;   unprintable characters (used for SCRAM-SHA-256 authentication).


;;; Code:

(require 'cl-lib)
(require 'eieio)
(require 'hex-util)
(require 'bindat)
(require 'url)
(require 'peg)
(require 'rx)
(require 'parse-time)
(require 'gnutls)
(require 'network-stream)


;; https://www.postgresql.org/docs/current/libpq-envars.html
(defvar pg-application-name (or (getenv "PGAPPNAME") "pg.el")
  "The application_name sent to the PostgreSQL backend.
This information appears in queries to the `pg_stat_activity' table
and (depending on server configuration) in the connection log.")

;; https://en.wikipedia.org/wiki/Null_(SQL)
(defvar pg-null-marker nil
  "The value used to represent the SQL NULL value")

(defvar pg-connect-timeout
  (cl-case system-type
    (windows-nt 0)
    (ms-dos 0)
    (t 30))
  "Timeout in seconds for establishing the network connection to PostgreSQL.
If set to zero (the default on Microsoft Windows platforms), do not create
a timer to signal a connection timeout.")

(defvar pg-read-timeout 10
  "Timeout in seconds when reading data from PostgreSQL.")

(defvar pg-disable-type-coercion nil
  "*Non-nil disables the type coercion mechanism.
The default is nil, which means that data recovered from the database
is coerced to the corresponding Emacs Lisp type before being returned;
for example numeric data is transformed to Emacs Lisp numbers, and
booleans to booleans.

The coercion mechanism requires an initialization query to the
database, in order to build a table mapping type names to OIDs. This
option is provided mainly in case you wish to avoid the overhead of
this initial query. The overhead is only incurred once per Emacs
session (not per connection to the backend).")

(defvar pg-parameter-change-functions (list 'pg-handle-parameter-client-encoding)
  "List of handlers called when the backend informs us of a parameter change.
Each handler is called with three arguments: the connection to
the backend, the parameter name and the parameter value.")

(defvar pg-handle-notice-functions (list 'pg-log-notice)
  "List of handlers called when the backend sends us a NOTICE message.
Each handler is called with one argument, the notice, as a pgerror
struct.")

(defvar pg-new-connection-hook (list #'pg-detect-server-variant)
  "A list of functions called when a new PostgreSQL connection is established.
Each function is called with the new connection as a single argument.

The default value of this hook includes a function that detects various
semi-compatible PostgreSQL variants, which sometimes requires additional
SQL queries. To avoid this overhead on establishing a connection, remove
`pg-detect-server-variant' from this list.")

(defvar pg-connection-buffer-octets (* 10 1024 1024)
  "Maximum length in octets of buffers used for PostgreSQL connections.")

;; See https://www.postgresql.org/docs/17/errcodes-appendix.html
(define-error 'pg-error "PostgreSQL error" 'error)
(define-error 'pg-user-error "pg-el user error" 'pg-error)
(define-error 'pg-protocol-error "PostgreSQL protocol error" 'pg-error)
(define-error 'pg-database-error "PostgreSQL database error" 'pg-error)
(define-error 'pg-operational-error "PostgreSQL operational error" 'pg-error)
(define-error 'pg-programming-error "PostgreSQL programming error" 'pg-error)
(define-error 'pg-data-error "PostgreSQL data error" 'pg-error)
(define-error 'pg-integrity-error "PostgreSQL integrity error" 'pg-error)
(define-error 'pg-internal-error "PostgreSQL internal error" 'pg-error)

(define-error 'pg-encoding-error "Client-level error encoding PostgreSQL query" 'pg-operational-error)
(define-error 'pg-connection-error "PostgreSQL connection failure" 'pg-operational-error)
(define-error 'pg-invalid-password "PostgreSQL invalid password" 'pg-operational-error)
(define-error 'pg-invalid-catalog-name "PostgreSQL invalid catalog name" 'pg-operational-error)
(define-error 'pg-feature-not-supported "PostgreSQL feature not supported" 'pg-error)
(define-error 'pg-syntax-error "PostgreSQL syntax error" 'pg-programming-error)
(define-error 'pg-undefined-table "PostgreSQL undefined table" 'pg-programming-error)
(define-error 'pg-undefined-column "PostgreSQL undefined column" 'pg-programming-error)
(define-error 'pg-undefined-function "PostgreSQL undefined function" 'pg-programming-error)
(define-error 'pg-duplicate-column "Duplicate column" 'pg-programming-error)
(define-error 'pg-duplicate-prepared-statement "Duplicate prepared statement" 'pg-programming-error)
(define-error 'pg-invalid-sql-statement-name "Invalid SQL statement name" 'pg-programming-error)
(define-error 'pg-invalid-cursor-name "Invalid cursor name" 'pg-programming-error)
(define-error 'pg-duplicate-table "Duplicate table or sequence" 'pg-programming-error)
(define-error 'pg-reserved-name "PostgreSQL reserved name" 'pg-programming-error)
(define-error 'pg-copy-failed "PostgreSQL COPY failed" 'pg-operational-error)
(define-error 'pg-connect-timeout "PostgreSQL connection attempt timed out" 'pg-operational-error)
(define-error 'pg-timeout "PostgreSQL data transfer timed out" 'pg-operational-error)
(define-error 'pg-type-error
              "Incorrect type in binding PostgreSQL prepared statement"
              'pg-user-error)
(define-error 'pg-numeric-value-out-of-range "PostgreSQL numeric value out of range" 'pg-data-error)
(define-error 'pg-division-by-zero "PostgreSQL division by zero" 'pg-data-error)
(define-error 'pg-floating-point-exception "PostgreSQL floating point exception" 'pg-data-error)
(define-error 'pg-array-subscript-error "PostgreSQL array subscript error" 'pg-data-error)
(define-error 'pg-datetime-field-overflow "PostgreSQL datetime field overflow" 'pg-data-error)
(define-error 'pg-character-not-in-repertoire "PostgreSQL character not in repertoire" 'pg-data-error)
(define-error 'pg-invalid-text-representation "Invalid text representation" 'pg-data-error)
(define-error 'pg-invalid-binary-representation "Invalid binary representation" 'pg-data-error)
(define-error 'pg-datatype-mismatch "PostgreSQL datatype mismatch" 'pg-data-error)
(define-error 'pg-json-error "PostgreSQL JSON-related error" 'pg-data-error)
(define-error 'pg-xml-error "PostgreSQL XML-related error" 'pg-data-error)
(define-error 'pg-sequence-limit-exceeded "PostgreSQL sequence generator limit exceeded" 'pg-data-error)
(define-error 'pg-integrity-constraint-violation "PostgreSQL integrity constraint violation" 'pg-integrity-error)
(define-error 'pg-restrict-violation "PostgreSQL restrict violation" 'pg-integrity-error)
(define-error 'pg-not-null-violation "PostgreSQL not NULL violation" 'pg-integrity-error)
(define-error 'pg-foreign-key-violation "PostgreSQL FOREIGN KEY violation" 'pg-integrity-error)
(define-error 'pg-unique-violation "PostgreSQL UNIQUE violation" 'pg-integrity-error)
(define-error 'pg-check-violation "PostgreSQL CHECK violation" 'pg-integrity-error)
(define-error 'pg-exclusion-violation "PostgreSQL exclusion violation" 'pg-integrity-error)
(define-error 'pg-transaction-missing "PostgreSQL no transaction in progress" 'pg-programming-error)
(define-error 'pg-transaction-timeout "PostgreSQL transaction timeout" 'pg-operational-error)
(define-error 'pg-insufficient-privilege "PostgreSQL insufficient privilege" 'pg-operational-error)
(define-error 'pg-insufficient-resources "PostgreSQL insufficient resources" 'pg-operational-error)
(define-error 'pg-disk-full "PostgreSQL disk full error" 'pg-operational-error)
(define-error 'pg-too-many-connections "PostgreSQL too many connections" 'pg-operational-error)
(define-error 'pg-plpgsql-error "PostgreSQL PL/pgSQL error" 'pg-programming-error)

(defun pg-signal-type-error (fmt &rest arguments)
  (let ((msg (apply #'format fmt arguments)))
    (signal 'pg-type-error (list msg))))


;; Maps from type-name to a function that converts from text representation to wire-level binary
;; representation.
(defvar pg--serializers (make-hash-table :test #'equal))

;; Contains an entry for types that serialize to a text format, rather than a binary format (e.g.
;; HSTORE). The serialization function itself is stored in pg--serializers.
(defvar pg--textual-serializers (make-hash-table :test #'equal))

;; Maps from type-name to a parsing function (from string to Emacs native type). This is built
;; dynamically at initialization of the connection with the database (once generated, the
;; information is shared between connections).
(defvar pg--parser-by-typname (make-hash-table :test #'equal))


(defclass pgcon ()
  ((dbname
    :type string
    :initarg :dbname
    :accessor pgcon-dbname)
   (process
    :initarg :process
    :accessor pgcon-process)
   (output-buffer
    :initform nil
    :accessor pgcon-output-buffer)
   (pid
    :type integer
    :accessor pgcon-pid)
   (server-version-major
    :accessor pgcon-server-version-major)
   ;; We use a minor-protocol version of 0 currently (but this can be changed using
   ;; the :protocol-version argument to `pg-connect-plist'), instead of version 2 introduced in
   ;; PostgreSQL v18, because several PostgreSQL variants refuse to accept a value different from
   ;; zero (despite the wire protocol having been designed to allow the service to request a
   ;; fallback to the latest minor protocol version that it knows how to handle).
   (minor-protocol-version
    :type integer
    :initform 0
    :accessor pgcon-minor-protocol-version)
   ;; Holds something like 'postgresql, 'ydb, 'cratedb
   (server-variant
    :type symbol
    :initform 'postgresql
    :accessor pgcon-server-variant)
   ;; The query cancellation keys that are sent to us by the backend when we establish a connection.
   ;; They are 32 bits in length when using protocol version 3.0, and a variable length up to 256
   ;; bits when using protocol version 3.2 (supported by PostgreSQL v18 onwards).
   (secret
    :accessor pgcon-secret)
   (client-encoding
    :type symbol
    :initform 'utf-8
    :accessor pgcon-client-encoding)
   ;; Maps from oid (an integer) to a parsing function.
   (parser-by-oid
    :type hash-table
    :initform (make-hash-table :test #'eql)
    :accessor pgcon-parser-by-oid)
   ;; Maps from type-name to PostgreSQL oid, for PostgreSQL builtin types.
   (oid-by-typname
    :type hash-table
    :initform (make-hash-table :test #'equal)
    :accessor pgcon-oid-by-typname)
   ;; Maps from oid to type-name.
   (typname-by-oid
    :type hash-table
    :initform (make-hash-table :test #'eql)
    :accessor pgcon-typname-by-oid)
   (timeout
    ;; This bizarre (progn ...) syntax is required by EIEIO for historical reasons.
    :initform (progn pg-read-timeout)
    :accessor pgcon-timeout)
   (connect-timer
    :initform nil
    :accessor pgcon-connect-timer)
   (query-log
    :initform nil
    :accessor pgcon-query-log)
   (prepared-statement-cache
    :type hash-table
    :initform (make-hash-table :test #'equal)
    :accessor pgcon-prepared-statement-cache)
   (connect-info
    :initform nil
    :accessor pgcon-connect-info)))

(defun make-pgcon (&rest args)
  (apply #'make-instance (cons 'pgcon args)))

(cl-defmethod cl-print-object ((this pgcon) stream)
  "Printer for pgcon PostgreSQL connection objects."
  (let ((dbname (when (slot-boundp this 'dbname) (pgcon-dbname this)))
        (pid (when (slot-boundp this 'pid) (pgcon-pid this))))
    (princ (format "#<PostgreSQL connection to %s, pid %s>"
                   (or dbname "<db not set>")
                   (or pid "<unknown>"))
           stream)))

;; Used to save the connection-specific position in the input and output buffers.
(defvar-local pgcon--position 1)

;; Used to check whether the connection is currently "busy", so that we can determine whether a
;; message was received asynchronously or synchronously.
(defvar-local pgcon--busy t)

(defvar-local pgcon--notification-handlers (list))


(defun pg-connection-set-busy (con busy)
  (with-current-buffer (process-buffer (pgcon-process con))
    (setq-local pgcon--busy busy)))

(defun pg-connection-busy-p (con)
  (with-current-buffer (process-buffer (pgcon-process con))
    pgcon--busy))

(defun pg-enable-query-log (con)
  "Enable logging of PostgreSQL queries on connection CON.
Queries are logged to a buffer identified by `pgcon-query-log'."
  (unless (pgcon-query-log con)
    (message "pg-el: enabling query log on %s" (cl-prin1-to-string con))
    (setf (pgcon-query-log con) (generate-new-buffer " *PostgreSQL query log*"))))

;; The qualified name is represented in SQL queries as schema.name. The schema is often either the
;; username or "public".
(cl-defstruct pg-qualified-name
  "The identifier for a table or view which optionally includes a schema."
  (schema nil)
  name)

;; Print as "\"schema\".\"name\"", for example "\"public\".\"mytable\"".
(defun pg-print-qualified-name (qn)
  (let ((schema (pg-escape-identifier (pg-qualified-name-schema qn)))
        (name (pg-escape-identifier (pg-qualified-name-name qn))))
    (if schema
        (format "%s.%s" schema name)
      name)))

(cl-defstruct pgresult
  connection status attributes tuples portal (incomplete nil))

(defmacro with-pg-connection-plist (con connect-plist &rest body)
  "Execute BODY forms in a scope with connection CON created by CONNECT-PLIST.
The database connection is bound to the variable CON. If the
connection is unsuccessful, the forms are not evaluated.
Otherwise, the BODY forms are executed, and upon termination,
normal or otherwise, the database connection is closed."
  (declare (indent defun))
  `(let ((,con (pg-connect-plist ,@connect-plist)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg-disconnect ,con)))))

(defmacro with-pg-connection (con connect-args &rest body)
  "Execute BODY forms in a scope with connection CON created by CONNECT-ARGS.
The database connection is bound to the variable CON. If the
connection is unsuccessful, the forms are not evaluated.
Otherwise, the BODY forms are executed, and upon termination,
normal or otherwise, the database connection is closed."
  (declare (indent defun)
           (obsolete with-pg-connection-plist "2025"))
  `(let ((,con (pg-connect ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg-disconnect ,con)))))

(defmacro with-pg-connection-local (con connect-args &rest body)
  "Execute BODY forms in a scope with local Unix connection CON
created by CONNECT-ARGS.
The database connection is bound to the variable CON. If the
connection is unsuccessful, the forms are not evaluated.
Otherwise, the BODY forms are executed, and upon termination,
normal or otherwise, the database connection is closed."
  (declare (indent defun))
  `(let ((,con (pg-connect-local ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg-disconnect ,con)))))


(defmacro with-pg-transaction (con &rest body)
  "Execute BODY forms in a BEGIN..END block with pre-established connection CON.
If a PostgreSQL error occurs during execution of the forms, execute
a ROLLBACK command.
Large-object manipulations _must_ occur within a transaction, since
the large object descriptors are only valid within the context of a
transaction."
  (declare (indent defun))
  (let ((exc-sym (gensym)))
    `(progn
       (pg-exec ,con "BEGIN")
       (condition-case ,exc-sym
           (prog1 (progn ,@body)
             (pg-exec ,con "COMMIT"))
         (error
          (message "PostgreSQL error %s" ,exc-sym)
          (pg-exec ,con "ROLLBACK"))))))
(put 'with-pg-transaction 'lisp-indent-function 'defun)

(defun pg-for-each (con select-form callback)
  "Create a cursor for SELECT-FORM and call CALLBACK for each result.
Uses the PostgreSQL database connection CON. SELECT-FORM must be an
SQL SELECT statement. The cursor is created using an SQL DECLARE
CURSOR command, then results are fetched successively until no results
are left. The cursor is then closed.

The work is performed within a transaction. The work can be
interrupted before all tuples have been handled by THROWing to a
tag called pg-finished."
  (let ((cursor (symbol-name (gensym "pgelcursor"))))
    (catch 'pg-finished
      (with-pg-transaction con
         (pg-exec con "DECLARE " cursor " CURSOR FOR " select-form)
         (unwind-protect
             (cl-loop for res = (pg-result (pg-exec con "FETCH 1 FROM " cursor) :tuples)
                   until (zerop (length res))
                   do (funcall callback res))
           (pg-exec con "CLOSE " cursor))))))

;; This is installed as an Emacs Lisp process filter for the PostgreSQL connection. We return nil if
;; the PostgreSQL connection is currently "busy" (meaning that we are currently processing a
;; synchronous request), or if the data received doesn't look like a complete NotificationResponse
;; message (starting with ?A, total length compatible with the length specified in the PostgreSQL
;; message format). Otherwise, we return the length of the message length that we handled.
;;
;; When we return non-nil, the original process filter is called (see `add-function' advice below),
;; which places the data in the process buffer for normal (synchronous) handling.
(defun pg-process-filter (process data)
  (with-current-buffer (process-buffer process)
    (unless pgcon--busy
      (when (and (eql ?A (aref data 0))
                 (eql 0 (aref data 1)))
        (let ((msglen 0))
          ;; read a net int in 4 octets representing the message length
          (setq msglen (+ (* 256 msglen) (aref data 1)))
          (setq msglen (+ (* 256 msglen) (aref data 2)))
          (setq msglen (+ (* 256 msglen) (aref data 3)))
          (setq msglen (+ (* 256 msglen) (aref data 4)))
          ;; We parse the channel and payload if we received a full NotificationResponse. msglen is one
          ;; less than the size of data due to the ?A message tag.
          (when (eql (1+ msglen) (length data))
            ;; ignore a net int in 4 octets representing notifying backend PID
            (let* ((channel-end-pos (cl-position 0 data :start 9 :end (length data)))
                   (payload-end-pos (cl-position 0 data :start (1+ channel-end-pos) :end (length data)))
                   (channel (cl-subseq data 9 channel-end-pos))
                   (payload (cl-subseq data (1+ channel-end-pos) payload-end-pos)))
              (dolist (handler pgcon--notification-handlers)
                (funcall handler channel payload))))
          (1- msglen))))))

;; With the :before-until advice type, the original process filter (which places the incoming
;; content in the process buffer) will be called only if our process filter returns nil. Our process
;; filter returns non-nil when it has detected, parsed and handled an asynchronous notification and
;; nil otherwise. This allows us to avoid duplicate processing of asynchronous notifications, once
;; by #'pg-process-filter and once by the notification handling code in pg-exec.
(defun pg-enable-async-notification-handlers (con)
  (add-function :before-until (process-filter (pgcon-process con)) #'pg-process-filter))

(defun pg-disable-async-notification-handlers (con)
  (remove-function (process-filter (pgcon-process con)) #'pg-process-filter))


(cl-defgeneric pg-do-variant-specific-setup (con variant)
  "Run any setup actions on connnection establishment specific to VARIANT.
Uses PostgreSQL connection CON.")

;; QuestDB only supports text-encoded values in the extended query statement protocol, so for this
;; PostgreSQL variant we ignore any binary serializers that were registered using
;; pg--register-serializer.
(cl-defmethod pg-do-variant-specific-setup ((_con pgcon) (_variant (eql 'questdb)))
  (message "pg-el: running variant-specific setup for QuestDB")
  (setq pg--serializers (make-hash-table :test #'equal)))

;; Register the OIDs associated with these OmniDB-specific types, so that their types appear in
;; column metadata listings.
(cl-defmethod pg-do-variant-specific-setup ((con pgcon) (_variant (eql 'alloydb)))
  (message "pg-el: running variant-specific setup for AlloyDB Omni")
  ;; These type names are in the google_ml schema
  (pg-register-parser "model_family_type" #'pg-text-parser)
  (pg-register-parser "model_family_info" #'pg-text-parser)
  (pg-register-parser "model_provider" #'pg-text-parser)
  (pg-register-parser "model_type" #'pg-text-parser)
  (pg-register-parser "auth_type" #'pg-text-parser)
  (pg-register-parser "auth_info" #'pg-text-parser)
  (pg-register-parser "models" #'pg-text-parser)
  (pg-initialize-parsers con))

;; OctoDB defaults to starting up using the SQL_ASCII client encoding.
(cl-defmethod pg-do-variant-specific-setup ((con pgcon) (_variant (eql 'octodb)))
  (message "pg-el: running variant-specific setup for YottaDB Octo")
  (pg-set-client-encoding con "UTF8"))

(cl-defmethod pg-do-variant-specific-setup ((con pgcon) (_variant (eql 'yellowbrick)))
  (message "pg-el: running variant-specific setup for YellowBrick")
  ;; YellowBrick Community Edition has limited support for server encodings other than LATIN9;
  ;; databases can be created with UTF8 encoding, but that doesn't change the behaviour for the
  ;; databaseless SELECT statements that we use in our tests.
  (pg-set-client-encoding con "UTF8")
  (pg-exec con "SET enable_full_bytea = true")
  (pg-exec con "SET enable_full_json = true")
  (pg-exec con "SET ybd_allow_udf_creation = true"))

(cl-defmethod pg-do-variant-specific-setup ((con pgcon) (variant t))
  ;; This statement fails on ClickHouse (and the database immediately closes the connection!).
  (unless (member variant '(clickhouse datafusion stoolap))
    (pg-exec con "SET datestyle = 'ISO'")))

(defun pg-detect-server-variant (con)
  "Detect the flavour of PostgreSQL that we are connected to.
Uses connection CON. The variant can be accessed by `pgcon-server-variant'."
  (pcase (pgcon-server-variant con)
    ;; This is the default value, meaning we haven't yet identified a variant based on its backend
    ;; parameter values.
    ('postgresql
     (let ((version (pg-backend-version con)))
       (cond ((cl-search "CrateDB" version)
              (setf (pgcon-server-variant con) 'cratedb))
             ((cl-search "CockroachDB" version)
              (setf (pgcon-server-variant con) 'cockroachdb))
             ((cl-search "-YB-" version)
              (setf (pgcon-server-variant con) 'yugabyte))
             ((cl-search "QuestDB" version)
              (setf (pgcon-server-variant con) 'questdb))
             ((cl-search "GreptimeDB" version)
              (setf (pgcon-server-variant con) 'greptimedb))
             ((cl-search "RisingWave" version)
              (setf (pgcon-server-variant con) 'risingwave))
             ((cl-search "implemented by immudb" version)
              (setf (pgcon-server-variant con) 'immudb))
             ((cl-search "Greenplum" version)
              (setf (pgcon-server-variant con) 'greenplum))
             ((cl-search "(Materialize " version)
              (setf (pgcon-server-variant con) 'materialize))
             ((cl-search "Vertica Analytic" version)
              (setf (pgcon-server-variant con) 'vertica))
             ((cl-search "PolarDB " version)
              (setf (pgcon-server-variant con) 'polardb))
             ((cl-search "CedarDB " version)
              (setf (pgcon-server-variant con) 'cedardb))
             ((cl-search "Yellowbrick Database" version)
              (setf (pgcon-server-variant con) 'yellowbrick))
             ((cl-search "pgsqlite " version)
              (setf (pgcon-server-variant con) 'pgsqlite))
             ((cl-search "openGauss" version)
              (setf (pgcon-server-variant con) 'opengauss))
             ((cl-search "Apache DataFusion" version)
              (setf (pgcon-server-variant con) 'datafusion))
             ;; TODO: find a better detection method for ArcadeDB
             ((string-suffix-p "/main)" version)
              (setf (pgcon-server-variant con) 'arcadedb))
             ;; A more expensive test is needed for Google AlloyDB. If the `omni_disk_cache_enabled'
             ;; parameter is defined, the query will return "on" or "off" as a string, and if the
             ;; parameter is not defined the query (second argument meaning no-error) will return
             ;; '((nil)).
             ;;
             ;; We avoid using the two-argument version of the current_setting() function, because
             ;; it raises errors in various semi-compatible PostgreSQL variants that only implement
             ;; the single argument version. Instead, query the
             ;; pg_settings table.
             ;;
             ;;   "SELECT current_setting('omni_disk_cache_enabled', true)"
             ((let* ((res (ignore-errors
                            (pg-exec-prepared con "SELECT setting FROM pg_catalog.pg_settings WHERE name=$1"
                                              `(("omni_disk_cache_enabled" . "text")))))
                     (rows (and res (pg-result res :tuples))))
                (unless (null rows)
                  (setf (pgcon-server-variant con) 'alloydb))))
             ;; TODO: we could also detect CitusDB in the same way by checking for citus.cluster_name
             ;; setting for example, but in practice it is very PostgreSQL compatible so identifying
             ;; it as a variant doesn't seem mandatory.
             ((let* ((sql "SELECT 1 FROM information_schema.schemata WHERE schema_name=$1")
                     (res (ignore-errors
                            (pg-exec-prepared con sql '(("_timescaledb_catalog" . "text"))))))
                (and res (pg-result res :tuples)))
              (setf (pgcon-server-variant con) 'timescaledb)))))
    ('ydb
     (pg-exec con "SET search_path = 'public'")))
  (pg-do-variant-specific-setup con (pgcon-server-variant con)))

(defun pg-handle-error-response (con &optional context)
  "Handle an ErrorMessage from the backend we are connected to over CON.
Additional information CONTEXT can be optionally included in the error message
presented to the user."
  (let ((e (pg-read-error-response con))
        (extra (list)))
    (when (pgerror-detail e)
      (push (format "detail: %s" (pgerror-detail e)) extra))
    (when (pgerror-hint e)
      (push (format "hint: %s" (pgerror-hint e)) extra))
    (when (pgerror-table e)
      (push (format "table: %s" (pgerror-table e)) extra))
    (when (pgerror-column e)
      (push (format "column: %s" (pgerror-column e)) extra))
    (when (pgerror-file e)
      (push (format "file: %s" (pgerror-file e)) extra))
    (when (pgerror-line e)
      (push (format "line: %s" (pgerror-line e)) extra))
    (when (pgerror-routine e)
      (push (format "routine: %s" (pgerror-routine e)) extra))
    (when (pgerror-dtype e)
      (push (format "dtype: %s" (pgerror-dtype e)) extra))
    (when (pgerror-where e)
      (push (format "where: %s" (pgerror-where e)) extra))
    (when (pgerror-constraint e)
      (push (format "constraint name: %s" (pgerror-constraint e)) extra))
    ;; Now read the ReadyForQuery message. We don't always receive this immediately; for example if
    ;; an incorrect username is sent during startup, PostgreSQL sends an ErrorMessage then an
    ;; AuthenticationSASL message. In that case, unread the message type octet so that it can
    ;; potentially be handled after the error is signaled. Some databases like Clickhouse
    ;; immediately close their connection on error, so we ignore any errors here.
    (ignore-errors
      (let ((c (pg--read-char con)))
        (unless (member c '(?Z ?E))
          (message "Unexpected message type after ErrorMsg (error was %s): %s" e c)
          (pg--unread-char con)))
      ;; Read message length then status, which we discard.
      (pg--read-net-int con 4)
      (pg--read-char con))
    (let ((msg (format "%s%s: %s (%s)"
                       (pgerror-severity e)
                       (if context (concat " " context) "")
                       (pgerror-message e)
                       (string-join extra ", ")))
          ;; https://www.postgresql.org/docs/current/errcodes-appendix.html
          (error-type (pcase (pgerror-sqlstate e)
                        ("0A000" 'pg-feature-not-supported)
                        ((pred (lambda (v) (string-prefix-p "08" v))) 'pg-connection-error)
                        ("28P01" 'pg-invalid-password)
                        ("28000" 'pg-invalid-password)
                        ("22003" 'pg-numeric-value-out-of-range)
                        ("2202E" 'pg-array-subscript-error)
                        ("22008" 'pg-datetime-field-overflow)
                        ("22012" 'pg-division-by-zero)
                        ("22P01" 'pg-floating-point-exception)
                        ("2201E" 'pg-floating-point-exception)
                        ("2201F" 'pg-floating-point-exception)
                        ("22021" 'pg-character-not-in-repertoire)
                        ((pred (lambda (v) (string-prefix-p "2203" v))) 'pg-json-error)
                        ("2200H" 'pg-sequence-limit-exceeded)
                        ("2200L" 'pg-xml-error)
                        ("2200M" 'pg-xml-error)
                        ("2200N" 'pg-xml-error)
                        ("2200S" 'pg-xml-error)
                        ("2200T" 'pg-xml-error)
                        ("22P02" 'pg-invalid-text-representation)
                        ("22P03" 'pg-invalid-binary-representation)
                        ((pred (lambda (v) (string-prefix-p "22" v))) 'pg-data-error)
                        ("23000" 'pg-integrity-constraint-violation)
                        ("23001" 'pg-restrict-violation)
                        ("23502" 'pg-not-null-violation)
                        ("23503" 'pg-foreign-key-violation)
                        ("23505" 'pg-unique-violation)
                        ("23514" 'pg-check-violation)
                        ("23P01" 'pg-exclusion-violation)
                        ((pred (lambda (v) (string-prefix-p "23" v))) 'pg-integrity-error)
                        ("25P01" 'pg-transaction-missing)
                        ("25P04" 'pg-transaction-timeout)
                        ((pred (lambda (v) (string-prefix-p "2F" v))) 'pg-programming-error)
                        ((pred (lambda (v) (string-prefix-p "38" v))) 'pg-programming-error)
                        ((pred (lambda (v) (string-prefix-p "39" v))) 'pg-programming-error)
                        ((pred (lambda (v) (string-prefix-p "40" v))) 'pg-operational-error)
                        ("26000" 'pg-invalid-sql-statement-name)
                        ("34000" 'pg-invalid-cursor-name)
                        ("3D000" 'pg-invalid-catalog-name)
                        ("42000" 'pg-syntax-error)
                        ("42601" 'pg-syntax-error)
                        ("42P01" 'pg-undefined-table)
                        ("42P05" 'pg-duplicate-prepared-statement)
                        ("42P07" 'pg-duplicate-table)
                        ("42701" 'pg-duplicate-column)
                        ("42703" 'pg-undefined-column)
                        ("42804" 'pg-datatype-mismatch)
                        ("42883" 'pg-undefined-function)
                        ("42501" 'pg-insufficient-privilege)
                        ("42939" 'pg-reserved-name)
                        ((pred (lambda (v) (string-prefix-p "42" v))) 'pg-programming-error)
                        ("53000" 'pg-insufficient-resources)
                        ("53100" 'pg-disk-full)
                        ("53300" 'pg-too-many-connections)
                        ((pred (lambda (v) (string-prefix-p "53" v))) 'pg-operational-error)
                        ((pred (lambda (v) (string-prefix-p "54" v))) 'pg-operational-error)
                        ((pred (lambda (v) (string-prefix-p "57" v))) 'pg-operational-error)
                        ("P0000" 'pg-plpgsql-error)
                        ((pred (lambda (v) (string-prefix-p "P0" v))) 'pg-programming-error)
                        ((pred (lambda (v) (string-prefix-p "XX" v))) 'pg-internal-error)
                        (_ 'pg-error))))
      (signal error-type (list msg)))))

;; Run the startup interaction with the PostgreSQL database. Authenticate and read the connection
;; parameters. This function allows us to share code common to TCP and Unix socket connections to
;; the backend.
(cl-defun pg-do-startup (con dbname user password)
  "Handle the startup sequence to authenticate with PostgreSQL over CON.
Uses database DBNAME, user USER and password PASSWORD."
  ;; send the StartupMessage, as per https://www.postgresql.org/docs/current/protocol-message-formats.html
  (pg-connection-set-busy con t)
  (let ((packet-octets (+ 4 2 2
                          (1+ (length "user"))
                          (1+ (length user))
                          (1+ (length "database"))
                          (1+ (length dbname))
                          (1+ (length "application_name"))
                          (1+ (length pg-application-name))
                          (1+ (length "client_encoding"))
                          (1+ (length "UTF8"))
                          1)))
    (pg--send-uint con packet-octets 4)
    (pg--send-uint con 3 2)              ; Protocol major version = 3
    (pg--send-uint con 0 2)              ; Protocol minor version = 0
    (pg--send-string con "user")
    (pg--send-string con user)
    (pg--send-string con "database")
    (pg--send-string con dbname)
    (pg--send-string con "application_name")
    (pg--send-string con pg-application-name)
    (pg--send-string con "client_encoding")
    (pg--send-string con "UTF8")
    ;; A zero byte is required as a terminator after the last name/value pair.
    (pg--send-uint con 0 1)
    (pg-flush con))
  (when (pgcon-connect-timer con)
    (cancel-timer (pgcon-connect-timer con)))
  (cl-loop
   for c = (pg--read-char con) do
   (cl-case c
     ;; an ErrorResponse message
     (?E
      (pg-handle-error-response con "after StartupMessage"))

     ;; NegotiateProtocolVersion. Note that this message will not be sent by PostgreSQL if the
     ;; backend implements exactly the requested protocol version.
     (?v
      (let* ((_msglen (pg--read-net-int con 4))
             (protocol-major-supported (pg--read-net-int con 2))
             (protocol-minor-supported (pg--read-net-int con 2))
             (unrec-option-count (pg--read-net-int con 4)))
        (unless (eql protocol-major-supported 3)
          (let ((msg (format "PostgreSQL backend supports protocol major version %d, we only support version 3"
                             protocol-major-supported)))
            (signal 'pg-protocol-error (list msg))))
        ;; We support a minor version <= 2. If the backend only supports protocol version 3.0 (minor
        ;; version = 0), then we set this to 0 to make sure we talk a version that the backend
        ;; supports. The only message format that changes between version 3.0 and 3.2 is the
        ;; BackendKeyData, which we can handle in the same manner in both protocol versions, but we
        ;; retain this information on the connection object nonetheless.
        (setf (pgcon-minor-protocol-version con) (min protocol-minor-supported 2))
        ;; read the list of protocol options not supported by the server
        (dotimes (_ unrec-option-count)
          (warn "PostgreSQL backend does not support the option %s" (pg--read-string con 4096)))))

     ;; BackendKeyData
     (?K
      (let ((msglen (pg--read-net-int con 4)))
        (setf (pgcon-pid con) (pg--read-net-int con 4))
        (setf (pgcon-secret con) (pg--read-chars con (- msglen 8)))))

     ;; NoticeResponse
     (?N
      ;; a Notice response has the same structure and fields as an ErrorResponse
      (let ((notice (pg-read-error-response con)))
        (dolist (handler pg-handle-notice-functions)
          (funcall handler notice))))

     ;; ReadyForQuery message
     (?Z
      (let ((_msglen (pg--read-net-int con 4))
            (status (pg--read-char con)))
        ;; status is 'I' or 'T' or 'E', Idle or InTransaction or Error
        (when (eql ?E status)
          (message "PostgreSQL ReadyForQuery message with error status"))
        (and (not pg-disable-type-coercion)
             (zerop (hash-table-count (pgcon-parser-by-oid con)))
             (pg-initialize-parsers con))
        (dolist (f pg-new-connection-hook)
          (funcall f con))
        (pg-enable-async-notification-handlers con)
        (pg-connection-set-busy con nil)
        (cl-return-from pg-do-startup con)))

     ;; an authentication request
     (?R
      (let ((_msglen (pg--read-net-int con 4))
            (areq (pg--read-net-int con 4)))
        (cl-case areq
         ;; AuthenticationOK message
          (0
           ;; Continue processing server messages and wait for the ReadyForQuery
           ;; message
           nil)

          ;; AUTH_REQ_KRB4
          (1
           (signal 'pg-protocol-error '("Kerberos4 authentication not supported")))

          ;; AUTH_REQ_KRB5
          (2
           (signal 'pg-protocol-error '("Kerberos5 authentication not supported")))

          ;; AUTH_REQ_CLEARTEXT_PASSWORD
          (3
           ;; send a PasswordMessage
           (let ((password-string (if (functionp password)
                                      (funcall password)
                                    password)))
             (pg--send-char con ?p)
             (pg--send-uint con (+ 5 (length password-string)) 4)
             (pg--send-string con password-string))
           (pg-flush con))

         ;; AUTH_REQ_CRYPT
         (4
          (signal 'pg-protocol-error '("Crypt authentication not supported")))

         ;; AUTH_REQ_MD5
         (5
          (pg-do-md5-authentication con user password))

         ;; AuthenticationSASL request
         (10
          (pg-do-sasl-authentication con user password))

         (t
          (let ((msg (format "Can't do that type of authentication: %s" areq)))
            (signal 'pg-protocol-error (list msg)))))))

     ;; ParameterStatus
     (?S
      (let* ((msglen (pg--read-net-int con 4))
             (msg (pg--read-chars con (- msglen 4)))
             (items (split-string msg (unibyte-string 0)))
             (key (cl-first items))
             (val (cl-second items)))
        ;; ParameterStatus items sent by the backend include application_name,
        ;; DateStyle, in_hot_standby, integer_datetimes
        (when (> (length key) 0)
          (when (string= "server_version" key)
            ;; We need to accept a version string of the form "17beta1" as well as "16.1"
            (let* ((major (cl-first (split-string val "\\.")))
                   (major-numeric (apply #'string
                                         (cl-loop
                                          for c across major
                                          while (<= ?0 c ?9)
                                          collect c))))
              (setf (pgcon-server-version-major con) (cl-parse-integer major-numeric)))
            (when (cl-search "ydb stable" val)
              (setf (pgcon-server-variant con) 'ydb))
            (when (cl-search "-greptimedb-" val)
              (setf (pgcon-server-variant con) 'greptimedb))
            (when (cl-search "OrioleDB" val)
              (setf (pgcon-server-variant con) 'orioledb))
            (when (cl-search "(ReadySet)" val)
              (setf (pgcon-server-variant con) 'readyset))
            (when (cl-search "(Stoolap)" val)
              (setf (pgcon-server-variant con) 'stoolap)))
          ;; Now some somewhat ugly code to detect semi-compatible PostgreSQL variants, to allow us
          ;; to work around some of their behaviour that is incompatible with real PostgreSQL.
          (when (string= "session_authorization" key)
            ;; We could also look for the existence of the "xata" schema in pg-schemas
            (when (string-prefix-p "xata" val)
              (setf (pgcon-server-variant con) 'xata))
            (when (string= "PGAdapter" val)
              (setf (pgcon-server-variant con) 'spanner)))
          (when (string= "khnum_version" key)
            (setf (pgcon-server-variant con) 'thenile))
          (when (string-prefix-p "ivorysql." key)
            (setf (pgcon-server-variant con) 'ivorydb))
          (dolist (handler pg-parameter-change-functions)
            (funcall handler con key val)))))

     (t
      (let ((msg (format "Problem connecting: expected an authentication response, got %s" c)))
        (signal 'pg-protocol-error (list msg)))))))


;; OPTIONS is a string containing commandline-like options of the form "-c geqo=off" and
;; "--geqo=off". Return an alist of (name . value) pairs.
;;
;; Tests:
;; (pg--parse-connection-options "-c geqo=off")
;; (pg--parse-connection-options "  -c   geqo=off  ")
;; (pg--parse-connection-options "-c geqo=off -c bizzle=bazzle")
;; (pg--parse-connection-options "-c geqo=off --fooble=bazzle")
;; (pg--parse-connection-options "")
;; (pg--parse-connection-options "wrong=syntax to-be-refused")
(defun pg--parse-connection-options (options)
  (with-temp-buffer
    (insert options)
    (goto-char (point-min))
    (with-peg-rules
        ((main (* [space]) optionlist (* [space]) (eol))
         (optionlist (* option) (* (and (+ [space]) option)))
         (option (or longform shortform))
         (longform "--" (substring identifier) "=" (substring value)
                   `(ident val -- (cons ident val)))
         (shortform "-c" (+ [space]) (substring identifier) "=" (substring value)
                    `(ident val -- (cons ident val)))
         (identifier (+ (or [a-z] [A-Z] [0-9] (set "._"))))
         (value (+ (or [a-z] [A-Z] [0-9] (set ",.#/$\"")))))
      (peg-run (peg main)))))

;; OPTIONS is a string containing commandline-like options of the form "-c geqo=off" and
;; "--geqo=off". We parse it and send them to the backend using SET statements. Valid options are
;; listed in https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-SHELL
(defun pg-handle-connection-options (con option-string)
  (let ((options (pg--parse-connection-options option-string)))
    (unless options
      (signal 'pg-user-error (list (format "Could not parse PGOPTIONS value %s" option-string))))
    (dolist (option options)
      (condition-case e
          (pg-exec-prepared con "SELECT set_config($1, $2, false)"
                            `((,(car option) . "text")
                              (,(cdr option) . "text")))
        (pg-error
         (let ((msg (format "Failed to set connection option %s to %s: %s"
                            (car option) (cdr option) e)))
           (signal 'pg-operational-error (list msg))))))))

;; Avoid warning from the bytecode compiler
(declare-function gnutls-negotiate "gnutls.el")
(declare-function network-stream-certificate "network-stream.el")

(cl-defun pg-connect-plist (dbname
                            user
                            &key
                            (password "")
                            (host "localhost")
                            (port 5432)
                            (tls-options nil)
                            (direct-tls nil)
                            (server-variant nil)
                            (protocol-version (cons 3 0)))
  "Initiate a connection with the PostgreSQL backend over TCP.
Connect to the database DBNAME with the username USER, on PORT of HOST,
providing PASSWORD if necessary. PASSWORD may be either a string or a
zero-argument function which returns a string. Return a connection to
the database (as an opaque type). PORT defaults to 5432, HOST to
\"localhost\", and PASSWORD to an empty string.

If TLS-OPTIONS is non-NIL, attempt to establish an encrypted connection
to PostgreSQL passing TLS-OPTIONS to `gnutls-negotiate'. To use a
standard TLS connection without any specific options, use a value of
`t'. To use client certificates to authenticate the TLS connection, use
a value of TLS-OPTIONS of the form `(:keylist ((,key ,cert)))', where
`key' is the filename of the client certificate private key and `cert'
is the filename of the client certificate.

If DIRECT-TLS is non-nil, attempt to establish a \"direct\" TLS
connection to PostgreSQL using ALPN, rather than the STARTTLS-like
connection upgrade handshake. This connection establishment method,
introduced with PostgreSQL 18, avoids some round trips during connection
establishment and may be required by some PostgreSQL installations. This
requires ALPN support in your Emacs (available from version 31).

Parameter SERVER-VARIANT can be used to specify that we are
connecting to a specific semi-compatible PostgreSQL variant. This
may be useful if the variant cannot be autodetected by pg-el but
you would like to run specific code in
`pg-do-variant-specific-setup'.

Parameter PROTOCOL-VERSION allows you to specify as a (MAJOR . MINOR)
cons pair the version of the wire protocol to be used on connection
startup. MAJOR should be 3. MINOR will default to 0 but can be set to 2
to use the updated protocol features introduced with PostgreSQL version
18."
  (let* ((buf (generate-new-buffer " *PostgreSQL*"))
         (process (make-network-process :name "postgres"
                                        :buffer buf
                                        :host host
                                        :service port
                                        :coding nil))
         (con (make-pgcon :dbname dbname :process process))
         (cert (network-stream-certificate host port nil))
         (opts (append (list :process process)
                       (list :hostname host)
                       ;; see https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml#alpn-protocol-ids
                       (list :alpn-protocols (list "postgresql"))
                       (when cert (list :keylist cert))
                       (when (listp tls-options) tls-options))))
    (when server-variant
      (setf (pgcon-server-variant con) server-variant))
    ;; Emacs supports disabling the Nagle algorithm, i.e. enabling TCP_NODELAY on this connection as
    ;; of version 31.x. We do that if possible as it leads to a huge performance improvement for TCP
    ;; connections (increasing throughput by a factor of 12 in our test suite, for example).
    (when (featurep 'make-network-process :nodelay)
      (set-network-process-option process :nodelay t))
    (unless (zerop pg-connect-timeout)
      (setf (pgcon-connect-timer con)
            (run-at-time pg-connect-timeout nil
                         (lambda ()
                           (unless (memq (process-status process) '(open listen))
                             (delete-process process)
                             (kill-buffer buf)
                             (signal 'pg-connect-timeout
                                     (list "PostgreSQL connection timed out")))))))
    (with-current-buffer buf
      (set-process-coding-system process 'binary 'binary)
      (set-buffer-multibyte nil)
      (buffer-disable-undo)
      (setq-local pgcon--position 1
                  pgcon--busy t
                  pgcon--notification-handlers (list)))
    (setf (pgcon-output-buffer con)
          (generate-new-buffer " *PostgreSQL output buffer*"))
    (with-current-buffer (pgcon-output-buffer con)
      (set-buffer-multibyte nil)
      (buffer-disable-undo)
      (setq-local pgcon--position 1))
    (unless (eql 3 (car protocol-version))
      (error 'pg-user-error "This library only supports a major protocol version of 3"))
    (setf (pgcon-minor-protocol-version con) (cdr protocol-version))
    ;; Save connection info in the pgcon object, for possible later use by pg-cancel
    (setf (pgcon-connect-info con) (list :tcp host port dbname user password))
    (cond (direct-tls
           ;; Here we make a "direct" TLS connection to PostgreSQL, rather than the STARTTLS-like
           ;; connection upgrade handshake. This requires ALPN support in Emacs. This connection
           ;; mode is only support from PostgreSQL 18.
           (unless (gnutls-available-p)
             (signal 'pg-error '("Connecting over TLS requires GnuTLS support in Emacs")))
           (condition-case err
               (apply #'gnutls-negotiate opts)
             (gnutls-error
              (let ((msg (format "TLS error connecting to PostgreSQL: %s"
                                 (error-message-string err))))
                (signal 'pg-protocol-error (list msg))))))
          (tls-options 
           ;; Classical TLS connections to PostgreSQL are based on a custom STARTTLS-like connection
           ;; upgrade handshake. The frontend establishes an unencrypted network connection to the
           ;; backend over the standard port (normally 5432). It then sends an SSLRequest message,
           ;; indicating the desire to establish an encrypted connection. The backend responds
           ;; with ?S to indicate that it is able to support an encrypted connection. The frontend
           ;; then runs TLS negociation to upgrade the connection to an encrypted one.
           (unless (gnutls-available-p)
             (signal 'pg-error '("Connecting over TLS requires GnuTLS support in Emacs")))
           ;; send the SSLRequest message
           (pg--send-uint con 8 4)
           (pg--send-uint con 80877103 4)
           (pg-flush con)
           (let ((ch (pg--read-char con)))
             (unless (eql ?S ch)
               (let ((msg (format "Couldn't establish TLS connection to PostgreSQL: read char %s" ch)))
                 (signal 'pg-protocol-error (list msg)))))
           ;; FIXME could use tls-options as third arg to network-stream-certificate
           (let* ((cert (network-stream-certificate host port nil))
                  (opts (append (list :process process)
                                (list :hostname host)
                                (when cert (list :keylist cert))
                                (when (listp tls-options) tls-options))))
             (condition-case err
                 ;; now do STARTTLS-like connection upgrade
                 (apply #'gnutls-negotiate opts)
               (gnutls-error
                (let ((msg (format "TLS error connecting to PostgreSQL: %s" (error-message-string err))))
                  (signal 'pg-protocol-error (list msg))))))))
    ;; The remainder of the startup sequence is common to TCP and Unix socket connections.
    (pg-do-startup con dbname user password)
    ;; We can't handle PGOPTIONS in pg-do-startup, because that contains code shared with
    ;; pg-connect/string and pg-connect/uri, and for these other functions any value for the options
    ;; paramspec specified in the connection string or URI overrides the value of the environment
    ;; variable.
    (when-let* ((options (getenv "PGOPTIONS")))
      (pg-handle-connection-options con options))
    con))

(cl-defun pg-connect (dbname user
                             &optional
                             (password "")
                             (host "localhost")
                             (port 5432)
                             (tls-options nil)
                             (server-variant nil))
  "Initiate a connection with the PostgreSQL backend over TCP.
Connect to the database DBNAME with the username USER, on PORT of HOST,
providing PASSWORD if necessary. PASSWORD may be a either string, or a
zero-argument function that returns a string. Return a connection to the
database (as an opaque type). PORT defaults to 5432, HOST to
\"localhost\", and PASSWORD to an empty string. If TLS-OPTIONS is
non-NIL, attempt to establish an encrypted connection to PostgreSQL
passing TLS-OPTIONS to `gnutls-negotiate'.

To use client certificates to authenticate the TLS connection,
use a value of TLS-OPTIONS of the form `(:keylist ((,key
,cert)))', where `key' is the filename of the client certificate
private key and `cert' is the filename of the client certificate.
These are passed to GnuTLS.

Variable SERVER-VARIANT can be used to specify that we are
connecting to a specific semi-compatible PostgreSQL variant. This
may be useful if the variant cannot be autodetected by pg-el but
you would like to run specific code in
`pg-do-variant-specific-setup'."
  (declare (obsolete pg-connect-plist "2025"))
  (pg-connect-plist dbname user
                    :password password
                    :host host
                    :port port
                    :tls-options tls-options
                    :server-variant server-variant))

;; This function is deprecated; use the :direct-tls parameter on pg-connect instead.
(cl-defun pg-connect/direct-tls (dbname user
                                        &optional
                                        (password "")
                                        (host "localhost")
                                        (port 5432)
                                        (tls-options nil))
  "Initiate a direct TLS connection with the PostgreSQL backend over TCP.
Connect to the database DBNAME with the username USER, on PORT of HOST,
providing PASSWORD if necessary. PASSWORD may be either a string, or a
zero-argument function that returns a string. Return a connection to
the database (as an opaque type). PORT defaults to 5432, HOST to
\"localhost\", and PASSWORD to an empty string. The TLS-OPTIONS are
passed to GnuTLS."
  (declare (obsolete pg-connect-plist "2025"))
  (pg-connect-plist dbname user
                    :password password
                    :host host
                    :port port
                    :tls-options tls-options
                    :direct-tls t))

(cl-defun pg-connect-local (path dbname user &optional (password ""))
  "Initiate a connection with the PostgreSQL backend over local Unix socket PATH.
Connect to the database DBNAME with the username USER, providing
PASSWORD if necessary. PASSWORD may be either a string, or a
zero-argument function that returns a string. Return a connection to the
database (as an opaque type). PASSWORD defaults to an empty string."
  (let* ((buf (generate-new-buffer " *PostgreSQL*"))
         (process (make-network-process :name "postgres"
                                        :buffer buf
                                        :family 'local
                                        :service path
                                        :coding nil))
         (con (make-pgcon :dbname dbname :process process)))
    ;; Save connection info in the pgcon object, for possible later use by pg-cancel
    (setf (pgcon-connect-info con) (list :local path nil dbname user password))
    (with-current-buffer buf
      (set-process-coding-system process 'binary 'binary)
      (set-buffer-multibyte nil)
      (setq-local pgcon--position 1
                  pgcon--busy t
                  pgcon--notification-handlers (list)))
    (setf (pgcon-output-buffer con)
          (generate-new-buffer " *PostgreSQL output buffer*"))
    (with-current-buffer (pgcon-output-buffer con)
      (set-buffer-multibyte nil)
      (buffer-disable-undo)
      (setq-local pgcon--position 1))
    (pg-do-startup con dbname user password)
    ;; We can't handle PGOPTIONS in pg-do-startup, because that contains code shared with
    ;; pg-connect/string and pg-connect/uri, and for these other functions any value for the options
    ;; paramspec specified in the connection string or URI overrides the value of the environment
    ;; variable.
    (when-let* ((options (getenv "PGOPTIONS")))
      (pg-handle-connection-options con options))
    con))


;; see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
(defun pg-connect/string (connection-string)
  "Connect to PostgreSQL with parameters specified by CONNECTION-STRING.
A connection string is of the form `host=localhost port=5432
dbname=mydb'. We do not support all the parameter keywords supported by
libpq, such as those which specify particular aspects of the TCP
connection to PostgreSQL (e.g. keepalives_interval). The supported
keywords are `host', `hostaddr', `port', `dbname', `user', `password',
`sslmode' (partial support), `connect_timeout', `read_timeout',
`client_encoding', `application_name' and `options'."
  (let* ((components (split-string connection-string "[ \t]" t))
         (params (cl-loop
                  for c in components
                  for param-val = (split-string c "=" t "\s")
                  unless (eql 2 (length param-val))
                  do (signal 'pg-user-error (list (message "Invalid connection string component %s" c)))
                  collect (cons (cl-first param-val) (cl-second param-val))))
         (host (or (cdr (assoc "host" params))
                   (cdr (assoc "hostaddr" params))
                   (getenv "PGHOST")
                   (getenv "PGHOSTADDR")
                   "localhost"))
         (port (or (cdr (assoc "port" params))
                   (getenv "PGPORT")
                   5432))
         (dbname (or (cdr (assoc "dbname" params))
                     (getenv "PGDATABASE")
                     (signal 'pg-user-error
                             (list "Database name not specified in connection string or PGDATABASE environment variable"))))
         (user (or (cdr (assoc "user" params))
                   (getenv "PGUSER")
                   (signal 'pg-user-error
                           (list "User not specified in connection string or PGUSER environment variable"))))
         (password (or (cdr (assoc "password" params))
                       (getenv "PGPASSWORD")))
         (sslmode (or (cdr (assoc "sslmode" params))
                      (getenv "PGSSLMODE")))
         (tls (cond ((string= sslmode "disable") nil)
                    ((string= sslmode "allow") t)
                    ((string= sslmode "prefer") t)
                    ((string= sslmode "require") t)
                    ((string= sslmode "verify-ca")
                     (error "verify-ca sslmode not implemented"))
                    ((string= sslmode "verify-full")
                     (error "verify-full sslmode not implemented"))
                    ((cdr (assoc "requiressl" params)) t)
                    (t nil)))
         (connect-timeout-str (cadr (assoc "connect_timeout" params)))
         (connect-timeout (and connect-timeout-str (cl-parse-integer connect-timeout-str)))
         (pg-connect-timeout (or connect-timeout pg-connect-timeout))
         ;; This "read_timeout" is a non-standard extension that we implement
         (read-timeout-str (cadr (assoc "read_timeout" params)))
         (read-timeout (and read-timeout-str (cl-parse-integer read-timeout-str)))
         (pg-read-timeout (or read-timeout pg-read-timeout))
         (pg-application-name (or (cdr (assoc "application_name" params))
                                  pg-application-name))
         (client-encoding-str (cadr (assoc "client_encoding" params)))
         (client-encoding (and client-encoding-str
                               (pg-normalize-encoding-name client-encoding-str)))
         (options (or (cdr (assoc "options" params))
                      (getenv "PGOPTIONS")))
         (protocol-version-string (or (cdr (assoc "protocol_version" params))
                                      (getenv "PG_PROTOCOL_VERSION")))
         (protocol-version-major nil)
         (protocol-version-minor nil))
    ;; TODO: should handle sslcert, sslkey variables
    ;;
    ;; Some of the parameters are taken from our local variable bindings, but for other parameters we
    ;; need to set them explicitly in the pgcon object.
    (let ((con (pg-connect-plist dbname user
                                 :password password
                                 :host host
                                 :port port
                                 :tls-options tls)))
      (when client-encoding
        (setf (pgcon-client-encoding con) client-encoding))
      (when options
        (pg-handle-connection-options con options))
      (when protocol-version-string
        (cond ((string-match (rx string-start (group (1+ digit) "." (group (1+ digit)) string-end))
                             protocol-version-string)
               (setq protocol-version-major (cl-parse-integer (match-string 1))
                     protocol-version-minor (cl-parse-integer (match-string 2))))
              (t
               (let ((msg (format "Invalid protocol-version string %s" protocol-version-string)))
                 (error 'pg-user-error (list msg))))))
      (when protocol-version-major
        (unless (eql 3 (car protocol-version-major))
          (error 'pg-user-error "This library only supports a major protocol version of 3")))
      (when protocol-version-minor
        (setf (pgcon-minor-protocol-version con) protocol-version-minor))
      con)))

(defun pg-parse-url (url)
  "Adaptation of function `url-generic-parse-url' that does not downcase
the host component of the URL."
  (if (null url)
      (url-parse-make-urlobj)
    (with-temp-buffer
      ;; Don't let those temp-buffer modifications accidentally
      ;; deactivate the mark of the current-buffer.
      (let ((deactivate-mark nil))
        (set-syntax-table url-parse-syntax-table)
	(erase-buffer)
	(insert url)
	(goto-char (point-min))
        (let ((save-pos (point))
              scheme user pass host port file fragment full
              (inhibit-read-only t))

          ;; 3.1. Scheme
	  ;; This is nil for a URI that is not fully specified.
          (when (looking-at "\\([a-zA-Z][-a-zA-Z0-9+.]*\\):")
	    (goto-char (match-end 0))
            (setq save-pos (point))
	    (setq scheme (downcase (match-string 1))))

          ;; 3.2. Authority
          (when (looking-at "//")
            (setq full t)
            (forward-char 2)
            (setq save-pos (point))
            (skip-chars-forward "^/?#")
            (setq host (buffer-substring-no-properties save-pos (point)))
	    ;; 3.2.1 User Information
            (if (string-match "^\\([^@]+\\)@" host)
                (setq user (match-string 1 host)
                      host (substring host (match-end 0))))
            (if (and user (string-match "\\`\\([^:]*\\):\\(.*\\)" user))
                (setq pass (match-string 2 user)
                      user (match-string 1 user)))
            (cond
	     ;; IPv6 literal address.
	     ((string-match "^\\(\\[[^]]+\\]\\)\\(?::\\([0-9]*\\)\\)?$" host)
	      (setq port (match-string 2 host)
		    host (match-string 1 host)))
	     ;; Registered name or IPv4 address.
	     ((string-match ":\\([0-9]*\\)$" host)
	      (setq port (match-string 1 host)
		    host (substring host 0 (match-beginning 0)))))
	    (cond ((equal port "")
		   (setq port nil))
		  (port
		   (setq port (cl-parse-integer port)))))

	  ;; Now point is on the / ? or # which terminates the
	  ;; authority, or at the end of the URI, or (if there is no
	  ;; authority) at the beginning of the absolute path.

          (setq save-pos (point))
          (if (string= "data" scheme)
	      ;; For the "data" URI scheme, all the rest is the FILE.
	      (setq file (buffer-substring-no-properties save-pos (point-max)))
	    ;; For hysterical raisins, our data structure returns the
	    ;; path and query components together in one slot.
	    ;; 3.3. Path
	    (skip-chars-forward "^?#")
	    ;; 3.4. Query
	    (when (looking-at "\\?")
	      (skip-chars-forward "^#"))
	    (setq file (buffer-substring-no-properties save-pos (point)))
	    ;; 3.5 Fragment
	    (when (looking-at "#")
	      (let ((opoint (point)))
		(forward-char 1)
                (setq fragment (buffer-substring-no-properties (point) (point-max)))
		(delete-region opoint (point-max)))))

          (if (and host (string-match "%[0-9][0-9]" host))
              (setq host (url-unhex-string host)))
          (url-parse-make-urlobj scheme user pass host port file
				 fragment nil full))))))


;; postgresql://[userspec@][hostspec][/dbname][?paramspec]
;; Examples:
;;   - postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp&ssl=true
;;   - postgresql://%2Fvar%2Flib%2Fpostgresql/dbname
;;
;; https://www.postgresql.org/docs/current/libpq-connect.html
(defun pg-connect/uri (uri)
  "Connect to PostgreSQL with parameters specified by URI.
A connection URI is of the form
`postgresql://[userspec@][hostspec][/dbname][?paramspec]'. `userspec' is
of the form username:password. If hostspec is a string representing a
local path (e.g. `%2Fvar%2Flib%2Fpostgresql' with percent-encoding) then
it is interpreted as a Unix pathname used for a local Unix domain
connection. We do not support all the paramspec keywords supported by
libpq, such as those which specify particular aspects of the TCP
connection to PostgreSQL (e.g. keepalives_interval). The supported
paramspec keywords are `sslmode' (partial support), `connect_timeout',
`read_timeout', `application_name', `client_encoding' and `options'."
  (let* ((parsed (pg-parse-url uri))
         (scheme (url-type parsed)))
    (unless (or (string= "postgres" scheme)
                (string= "postgresql" scheme))
      (let ((msg (format "Invalid protocol %s in connection URI" scheme)))
        (signal 'pg-programming-error (list msg))))
    ;; FIXME unfortunately the url-host is being downcased by url-generic-parse-url, which is
    ;; incorrect when the hostname is specifying a local path.
    (let* ((host (url-unhex-string (url-host parsed)))
           (parsed-user (url-user parsed))
           (parsed-password (url-password parsed))
           (user (or (when parsed-user (url-unhex-string parsed-user))
                     (getenv "PGUSER")))
           (password (or (when parsed-password (url-unhex-string parsed-password))
                         (getenv "PGPASSWORD")))
           (port (or (url-portspec parsed)
                     (getenv "PGPORT")
                     5432))
           (path-query (url-path-and-query parsed))
           (dbname (or (and (car path-query)
                            ;; ignore the "/" prefix
                            (substring (car path-query) 1))
                       (getenv "PGDATABASE")
                       (signal 'pg-programming-error '("Missing database name in connection URI"))))
           (params (cdr path-query))
           ;; this is returning a list of lists, not an alist
           (params (and params (url-parse-query-string params)))
           (sslmode (or (cadr (assoc "sslmode" params))
                        (getenv "PGSSLMODE")))
           (tls (cond ((string= sslmode "disable") nil)
                      ((string= sslmode "allow") t)
                      ((string= sslmode "prefer") t)
                      ((string= sslmode "require") t)
                      ((string= sslmode "verify-ca")
                       (signal 'pg-error '("verify-ca sslmode not implemented")))
                      ((string= sslmode "verify-full")
                       (signal 'pg-error '("verify-full sslmode not implemented")))
                      ((cdr (assoc "requiressl" params)) t)
                      (t nil)))
           ;; Should be a decimal integer designating a number of seconds
           (connect-timeout-str (cadr (assoc "connect_timeout" params)))
           (connect-timeout (and connect-timeout-str (cl-parse-integer connect-timeout-str)))
           (pg-connect-timeout (or connect-timeout pg-connect-timeout))
           ;; This "read_timeout" is a non-standard extension that we implement
           (read-timeout-str (cadr (assoc "read_timeout" params)))
           (read-timeout (and read-timeout-str (cl-parse-integer read-timeout-str)))
           (pg-read-timeout (or read-timeout pg-read-timeout))
           (pg-application-name (or (cadr (assoc "application_name" params))
                                    pg-application-name))
           (client-encoding-str (cadr (assoc "client_encoding" params)))
           (client-encoding (and client-encoding-str
                                 (pg-normalize-encoding-name client-encoding-str)))
           (options (or (cadr (assoc "options" params))
                        (getenv "PGOPTIONS"))))
      ;; If the host is empty or looks like an absolute pathname, connect over Unix-domain socket.
      (let ((con (if (or (zerop (length host))
                         (eq ?/ (aref host 0)))
                     (pg-connect-local host dbname user password)
                   (pg-connect-plist dbname user
                                     :password password
                                     :host host
                                     :port port
                                     :tls-options tls))))
        (when client-encoding
          (setf (pgcon-client-encoding con) client-encoding))
        (when options
          (pg-handle-connection-options con options))
        con))))


;; Called from pg-parameter-change-functions when we receive a ParameterStatus
;; message of type name=value from the backend. If the status message concerns
;; the client encoding, update the value recorded in the connection.
(defun pg-handle-parameter-client-encoding (con name value)
  (when (string= "client_encoding" name)
    (let ((ce (pg-normalize-encoding-name value)))
      (if ce
          (setf (pgcon-client-encoding con) ce)
        (let ((msg (format "Don't know the Emacs equivalent for client encoding %s" value)))
          (signal 'pg-error (list msg)))))))

(defun pg-add-notification-handler (con handler)
  "Register HANDLER for NotificationResponse messages on CON.
A handler takes two arguments: the channel and the payload. These
correspond to SQL-level NOTIFY channel, \\='payload\\='."
  (with-current-buffer (process-buffer (pgcon-process con))
    (push handler pgcon--notification-handlers)))

(defun pg--trim-connection-buffers (con)
  "Trim the input and output buffers for CON if needed.
For long-running PostgreSQL connections, the input buffer and output
buffer may become very large over time. Ensure that we only retain
pg-connection-buffer-octets octets for each of these buffers."
  (with-current-buffer (process-buffer (pgcon-process con))
    ;; Our buffer is unibyte, so the number of characters returned by buffer-size is also a number
    ;; of octets.
    (when (> (buffer-size) pg-connection-buffer-octets)
      (let* ((start-deletion (point-min))
             (end-deletion (- (point-max) pg-connection-buffer-octets))
             (removed (- end-deletion start-deletion)))
      (delete-region start-deletion end-deletion)
      ;; pgcon--position is a buffer-local variable
      (cl-decf pgcon--position removed))))
  (when (pgcon-output-buffer con)
    (with-current-buffer (pgcon-output-buffer con)
      (when (> (buffer-size) pg-connection-buffer-octets)
        (let* ((start-deletion (point-min))
               (end-deletion (- (point-max) pg-connection-buffer-octets))
               (removed (- end-deletion start-deletion)))
        (delete-region start-deletion end-deletion)
        (cl-decf pgcon--position removed))))))

(cl-defun pg-exec (con &rest args)
  "Execute the SQL command given by concatenating ARGS on database CON.
Return a result structure which can be decoded using `pg-result'."
  (pg-connection-set-busy con t)
  (let* ((sql (apply #'concat args))
         (tuples (list))
         (attributes (list))
         (result (make-pgresult :connection con))
         (ce (pgcon-client-encoding con))
         (encoded (if ce (encode-coding-string sql ce t) sql)))
    ;; Ensure that the SQL string is encodable with the current client-encoding system. The function
    ;; `encode-coding-string' does not trigger an error, but rather silently replaces characters
    ;; that can't be encoded with '?' or ' '; we don't want to send this corrupted SQL string to
    ;; PostgreSQL. Unfortunately there does not seem to be any more efficient solution than
    ;; searching through the list returned by `find-coding-systems-string'. Also note that this
    ;; check only works if we are using the canonical name for an encoding system, and doesn't work
    ;; for coding system aliases (eg. 'latin-1 for 'iso-latin-1). We ensure that we are not using
    ;; aliases in the values contained in pg--encoding-names.
    (when ce
      (let ((codings (find-coding-systems-string sql)))
        (unless (or (eq 'undecided (car codings))
		    (memq ce codings))
          (let ((msg (format "Can't encode `%s' with current client encoding %s" sql ce)))
	    (signal 'pg-encoding-error (list msg))))))
    ;; (message "pg-exec: %s" sql)
    (when (pgcon-query-log con)
      (with-current-buffer (pgcon-query-log con)
        (insert sql "\n"))
      (when noninteractive
        (message "SQL:> %s" sql)))
    (pg--trim-connection-buffers con)
    (let ((len (length encoded)))
      (when (> len (- (expt 2 32) 5))
        (signal 'pg-user-error (list "Query is too large")))
      (pg--send-char con ?Q)
      (pg--send-uint con (+ 4 len 1) 4)
      (pg--send-string con encoded)
      (pg-flush con))
    (cl-loop for c = (pg--read-char con) do
       ;; (message "pg-exec message-type = %c" c)
       (cl-case c
            ;; NoData
            (?n
             (pg--read-net-int con 4))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg--read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg--read-int con 4))
                    (channel (pg--read-string con))
                    (payload (pg--read-string con))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; Bind -- should not receive this here
            (?B
             (unless attributes
               (signal 'pg-protocol-error (list "Tuple received before metadata")))
             (let ((_msglen (pg--read-net-int con 4)))
               (push (pg--read-tuple con attributes) tuples)))

            ;; CommandComplete -- one SQL command has completed
            (?C
             (let* ((msglen (pg--read-net-int con 4))
                    (msg (pg--read-chars con (- msglen 5)))
                    (_null (pg--read-char con)))
               (setf (pgresult-status result) msg)))
               ;; now wait for the ReadyForQuery message

            ;; DataRow
            (?D
             (let ((_msglen (pg--read-net-int con 4))
                   (tuple (pg--read-tuple con attributes)))
               (push tuple tuples)))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con "in pg-exec"))

            ;; EmptyQueryResponse -- response to an empty query string
            (?I
             (pg--read-net-int con 4)
             (setf (pgresult-status result) "EMPTY"))

            ;; BackendKeyData
            (?K
             (let ((msglen (pg--read-net-int con 4)))
               (setf (pgcon-pid con) (pg--read-net-int con 4))
               (setf (pgcon-secret con) (pg--read-chars con (- msglen 8)))))

            ;; NoticeResponse
            (?N
             ;; a Notice response has the same structure and fields as an ErrorResponse
             (let ((notice (pg-read-error-response con)))
               ;; This is rather ugly, but seems to be the only way of detecting YottaDB Octo on startup.
               (when (string= "INFO" (pgerror-severity notice))
                 (when (string-prefix-p "Generating M file [" (pgerror-message notice))
                   (setf (pgcon-server-variant con) 'octodb)))
               (dolist (handler pg-handle-notice-functions)
                 (funcall handler notice))))

            ;; CursorResponse
            (?P
             (let ((portal (pg--read-string con)))
               (setf (pgresult-portal result) portal)))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg--read-net-int con 4))
                    (msg (pg--read-chars con (- msglen 4)))
                    (items (split-string msg (unibyte-string 0))))
               ;; ParameterStatus items sent by the backend include application_name,
               ;; DateStyle, TimeZone, in_hot_standby, integer_datetimes
               (when (> (length (cl-first items)) 0)
                 (dolist (handler pg-parameter-change-functions)
                   (funcall handler con (cl-first items) (cl-second items))))))

            ;; RowDescription
            (?T
             (when attributes
               (signal 'pg-protocol-error (list "Cannot handle multiple result group")))
             (setq attributes (pg--read-attributes con)))

            ;; CopyFail
            (?f
             (let* ((msglen (pg--read-net-int con 4))
                    (msg (pg--read-chars con (- msglen 4))))
               (message "Unexpected CopyFail message %s" msg)))

            ;; ParseComplete -- not expecting this using the simple query protocol
            (?1
             (pg--read-net-int con 4))

            ;; BindComplete -- not expecting this using the simple query protocol
            (?2
             (pg--read-net-int con 4))

            ;; CloseComplete -- not expecting this using the simple query protocol
            (?3
             (pg--read-net-int con 4))

            ;; PortalSuspended -- this message is not expected using the simple query protocol
            (?s
             (message "Unexpected PortalSuspended message in pg-exec (sql was %s)" sql)
             (pg--read-net-int con 4)
             (setf (pgresult-incomplete result) t)
             (setf (pgresult-tuples result) (nreverse tuples))
             (setf (pgresult-status result) "SUSPENDED")
             (pg-connection-set-busy con nil)
             (cl-return-from pg-exec result))

            ;; ReadyForQuery
            (?Z
             (let ((_msglen (pg--read-net-int con 4))
                   (status (pg--read-char con)))
               ;; status is 'I' or 'T' or 'E', Idle or InTransaction or Error
               (when (eql ?E status)
                 (message "PostgreSQL ReadyForQuery message with error status"))
               (setf (pgresult-tuples result) (nreverse tuples))
               (setf (pgresult-attributes result) attributes)
               (pg-connection-set-busy con nil)
               (cl-return-from pg-exec result)))

            (t
             (let ((msg (format "Unknown response type from backend in pg-exec: %s" c)))
               (signal 'pg-protocol-error (list msg))))))))


(defun pg-result (result what &rest arg)
  "Extract WHAT component of RESULT.
RESULT should be a structure obtained from a call to `pg-exec',
and the keyword WHAT should be one of
   :connection -> return the connection object
   :status -> return the status string provided by the database
   :attributes -> return the metadata, as a list of lists
   :incomplete -> are more rows pending in the portal
   :tuples -> return the data, as a list of lists
   :tuple n -> return the nth component of the data
   :oid -> return the OID (a unique identifier generated by PostgreSQL
           for each row resulting from an insertion)"
  (cl-case what
    (:connection (pgresult-connection result))
    (:status     (pgresult-status result))
    (:attributes (pgresult-attributes result))
    (:incomplete (pgresult-incomplete result))
    (:tuples     (pgresult-tuples result))
    (:tuple
     (let ((which (if (integerp (car arg)) (car arg)
                    (let ((msg (format "%s is not an integer" arg)))
                      (signal 'pg-programming-error (list msg)))))
           (tuples (pgresult-tuples result)))
       (nth which tuples)))
    (:oid
     (let ((status (pgresult-status result)))
       (if (string-prefix-p "INSERT" status)
           (cl-parse-integer (substring status 7 (cl-position ? status :start 7)))
         (let ((msg (format "Only INSERT commands generate an oid: %s" status)))
           (signal 'pg-programming-error (list msg))))))
    (t
     (let ((msg (format "Unknown result request %s" what)))
       (signal 'pg-programming-error (list msg))))))


(defun pg--escape-identifier-simple (str)
  (with-temp-buffer
    (insert ?\")
    (cl-loop for c across str
             do (when (eql c ?\") (insert ?\"))
             (insert c))
    (insert ?\")
    (buffer-string)))

;; Similar to libpq function PQescapeIdentifier.
;; See https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-EXEC-ESCAPE-STRING
;;
;; This function can help to prevent SQL injection attacks ("Little Bobby Tables",
;; https://xkcd.com/327/) in situations where you can't use a prepared statement (a parameterized
;; query, using the prepare/bind/execute extended query message flow in PostgreSQL). You might need
;; this for example when specifying the name of a column in a SELECT statement. See function
;; `pg-exec-prepared' which should be used when possible instead of relying on this function.
(defun pg-escape-identifier (identifier)
  "Escape and quote an SQL identifier (table, column, function name...).
IDENTIFIER can be a string or a pg-qualified-name (including a
schema specifier). Similar to libpq function PQescapeIdentifier.
You should use prepared statements (`pg-exec-prepared') instead
of this function whenever possible."
  (cond ((pg-qualified-name-p identifier)
         (let ((schema (pg-qualified-name-schema identifier))
               (name (pg-qualified-name-name identifier)))
           (if schema
               (format "%s.%s"
                       (pg--escape-identifier-simple schema)
                       (pg--escape-identifier-simple name))
             (pg--escape-identifier-simple name))))
        (t
         (pg--escape-identifier-simple identifier))))

(defun pg-escape-literal (string)
  "Escape STRING for use within an SQL command.
Similar to libpq function PQescapeLiteral. You should use
prepared statements (`pg-exec-prepared') instead of this function
whenever possible."
  (with-temp-buffer
    (insert ?E)
    (insert ?\')
    (cl-loop
     for c across string do
     (when (eql c ?\') (insert ?\'))
     (when (eql c ?\\) (insert ?\\))
     (insert c))
    (insert ?\')
    (buffer-string)))


;; We look up the type-name in our OID cache. If it's not found, we force a refresh of our OID
;; cache, because a new type might have been defined using CREATE TYPE, either in this session since
;; connection establishment when we populated the cache, or in a parallel connection to PostgreSQL.
(defun pg--lookup-oid (con type-name)
  "Return the PostgreSQL OID associated with TYPE-NAME.
This may force a refresh of the OID-typename cache if TYPE-NAME is not known.
Uses PostgreSQL connection CON."
  (let ((oid-by-typname (pgcon-oid-by-typname con)))
    (or (gethash type-name oid-by-typname)
        (progn
          (pg-initialize-parsers con)
          (gethash type-name oid-by-typname)))))

;; This version that errors on unknown OID is deprecated.
(defun pg--lookup-type-name (con oid)
  "Return the PostgreSQL type name associated with OID.
Uses PostgreSQL connection CON."
  (let ((typname-by-oid (pgcon-typname-by-oid con)))
    (or (gethash oid typname-by-oid)
        (progn
          (pg-initialize-parsers con)
          (or (gethash oid typname-by-oid)
              (let ((msg (format "Unknown PostgreSQL oid %d" oid)))
                (signal 'pg-error (list msg))))))))

;; TODO: this function does not work with the Vertica variant, where the pg_type table is not implemented.
(defun pg-lookup-type-name (con oid)
  "Return the PostgreSQL type name associated with OID.
Uses PostgreSQL connection CON."
  (let ((typname-by-oid (pgcon-typname-by-oid con)))
    (or (gethash oid typname-by-oid)
        (let* ((sql "SELECT typname FROM pg_catalog.pg_type WHERE oid=$1")
               (res (pg-exec-prepared con sql `((,oid . "int4"))))
               (maybe-name (cl-first (pg-result res :tuple 0))))
          (when maybe-name
            (setf (gethash oid typname-by-oid) maybe-name))
          maybe-name))))


(cl-defun pg-prepare (con query argument-types &key (name ""))
  "Prepare statement QUERY with ARGUMENT-TYPES on connection CON.
The prepared statement may be given optional NAME (defaults to an
unnamed prepared statement). ARGUMENT-TYPES is a list of
PostgreSQL type names of the form (\"int4\" \"text\" \"bool\").
Returns the prepared statement name (a string)."
  (when (pgcon-query-log con)
    (with-current-buffer (pgcon-query-log con)
      (insert query "\n")
      (insert (format "   %s\n" argument-types)))
    (when noninteractive
      (message "SQL!> %s %s" query argument-types)))
  (cl-flet ((oid-for (type-name)
              ;; If we have defined a serializer for type-name and we know the corresponding OID, we
              ;; will be sending this type in binary form: return that OID. Otherwise, return the
              ;; pseudo-OID value of 0 to tell PostgreSQL that we are sending this type in text
              ;; format.
              (if (gethash type-name pg--serializers)
                  (or (pg--lookup-oid con type-name)
                      (let ((msg (format "Don't know the OID for PostgreSQL type %s" type-name)))
                        (signal 'pg-error (list msg))))
                0)))
    (let* ((ce (pgcon-client-encoding con))
           (query/enc (if ce (encode-coding-string query ce t) query))
           (oids (mapcar #'oid-for argument-types))
           (len (+ 4 (1+ (length name)) (1+ (length query/enc)) 2 (* 4 (length oids)))))
      ;; send a Parse message
      (pg-connection-set-busy con t)
      (pg--send-char con ?P)
      (pg--send-uint con len 4)
      (pg--send-string con name)
      (pg--send-string con query/enc)
      (pg--send-uint con (length oids) 2)
      (dolist (oid oids)
        (pg--send-uint con oid 4))
      (pg-flush con))
    name))

(cl-defun pg-bind (con statement-name typed-arguments &key (portal ""))
  "Bind the SQL prepared statement STATEMENT-NAME to TYPED-ARGUMENTS.
The STATEMENT-NAME should have been returned by function `pg-prepare'.
TYPE-ARGUMENTS is a list of the form ((42 . \"int4\") (\"foo\" . \"text\")).
Uses PostgreSQL connection CON."
  (let* ((ce (pgcon-client-encoding con))
         (argument-values (mapcar #'car typed-arguments))
         (argument-types (mapcar #'cdr typed-arguments))
         (serialized-values
          (cl-loop
           for typ in argument-types
           for v in argument-values
           for serializer = (gethash typ pg--serializers)
           collect (cond ((gethash typ pg--textual-serializers)
                          ;; this argument will be sent as text
                          (cons (funcall serializer v ce) 0))
                         (serializer
                          ;; this argument will be sent in binary format
                          (cons (funcall serializer v ce) 1))
                         (t
                          ;; this argument will be sent in text format, raw
                          (let* ((raw (if (stringp v) v (format "%s" v)))
                                 (encoded (if ce (encode-coding-string raw ce t) raw)))
                            (cons encoded 0))))))
         (len (+ 4
                 (1+ (length portal))
                 (1+ (length statement-name))
                 2
                 (* 2 (length argument-types))
                 2
                 (cl-loop for v in (mapcar #'car serialized-values) sum (+ 4 (length v)))
                 2)))
    (when (> len (expt 2 32))
      (signal 'pg-user-error (list "Field is too large")))
    ;; send a Bind message
    (pg--send-char con ?B)
    (pg--send-uint con len 4)
    ;; the destination portal
    (pg--send-string con portal)
    (pg--send-string con statement-name)
    (pg--send-uint con (length argument-types) 2)
    (cl-loop for (_ . binary-p) in serialized-values
             do (pg--send-uint con binary-p 2))
    (pg--send-uint con (length argument-values) 2)
    (cl-loop
     for (v . _) in serialized-values
     do (if (null v)
            ;; for a null value, send -1 followed by zero octets for the value
            (pg--send-uint con -1 4)
          (let ((len (length v)))
            (when (> len (expt 2 32))
              (signal 'pg-user-error (list "Field is too large")))
            (pg--send-uint con len 4)
            (pg--send-octets con v))))
    ;; the number of result-column format codes: we use zero to indicate that result columns can use
    ;; text format
    (pg--send-uint con 0 2)
    (pg-flush con)
    portal))

(defun pg-describe-portal (con portal)
  (let ((len (+ 4 1 (1+ (length portal)))))
    ;; send a Describe message for this portal
    (pg--send-char con ?D)
    (pg--send-uint con len 4)
    (pg--send-char con ?P)
    (pg--send-string con portal)
    (pg-flush con)))

(cl-defun pg-execute (con portal &key (max-rows 0))
  (let* ((ce (pgcon-client-encoding con))
         (pn/encoded (if ce (encode-coding-string portal ce t) portal))
         (len (+ 4 (1+ (length pn/encoded)) 4)))
    ;; send an Execute message
    (pg--send-char con ?E)
    (pg--send-uint con len 4)
    ;; the destination portal
    (pg--send-string con pn/encoded)
    ;; Maximum number of rows to return; zero means "no limit"
    (pg--send-uint con max-rows 4)
    (pg-flush con)))

(cl-defun pg-fetch (con result &key (max-rows 0))
  "Fetch pending rows from portal in RESULT on database connection CON.
Retrieve at most MAX-ROWS rows (default value of zero means no limit).
Returns a pgresult structure (see function `pg-result')."
  (let* ((tuples (list))
         (attributes (pgresult-attributes result)))
    (setf (pgresult-status result) nil)
    ;; We are counting on the Describe message having been sent prior to calling pg-fetch.
    (pg-execute con (pgresult-portal result) :max-rows max-rows)
    ;; If we are requesting a subset of available rows, we send a Flush message instead of a Sync
    ;; message, otherwise our unnamed portal will be closed by the Sync message and we won't be able
    ;; to retrieve more rows on the next call to pg-fetch.
    (cond ((zerop max-rows)
           ;; send a Sync message
           (pg--send-char con ?S)
           (pg--send-uint con 4 4))
          (t
           ;; send a Flush message
           (pg--send-char con ?H)
           (pg--send-uint con 4 4)))
    (pg-flush con)
    ;; In the extended query protocol, the Execute phase is always terminated by the appearance of
    ;; exactly one of these messages: CommandComplete, EmptyQueryResponse (if the portal was created
    ;; from an empty query string), ErrorResponse, or PortalSuspended.
    (cl-loop
     for c = (pg--read-char con) do
     ;; (message "pg-fetch got %c" c)
     (cl-case c
       ;; ParseComplete
       (?1
        (pg--read-net-int con 4))

       ;; BindComplete
       (?2
        (pg--read-net-int con 4))

       ;; NotificationResponse
       (?A
        (let* ((_msglen (pg--read-net-int con 4))
               ;; PID of the notifying backend
               (_pid (pg--read-int con 4))
               (channel (pg--read-string con))
               (payload (pg--read-string con))
               (buf (process-buffer (pgcon-process con)))
               (handlers (with-current-buffer buf pgcon--notification-handlers)))
          (dolist (handler handlers)
            (funcall handler channel payload))))

       ;; ParameterStatus
       (?S
        (let* ((msglen (pg--read-net-int con 4))
               (msg (pg--read-chars con (- msglen 4)))
               (items (split-string msg (unibyte-string 0)))
               (key (cl-first items))
               (val (cl-second items)))
          ;; ParameterStatus items sent by the backend include application_name,
          ;; DateStyle, in_hot_standby, integer_datetimes
          (when (> (length key) 0)
            (dolist (handler pg-parameter-change-functions)
              (funcall handler con key val)))))

       ;; RowDescription
       (?T
        (when attributes
          (signal 'pg-protocol-error (list "Cannot handle multiple result group")))
        (setq attributes (pg--read-attributes con))
        (setf (pgresult-attributes result) attributes))

       ;; DataRow message
       (?D
        (let ((_msglen (pg--read-net-int con 4)))
          (push (pg--read-tuple con attributes) tuples)))

       ;; PortalSuspended -- the row-count limit for the Execute message was reached; more data is
       ;; available with another Execute message.
       (?s
        (unless (> max-rows 0)
          (message "Unexpected PortalSuspended message in pg-exec-prepared"))
        (pg--read-net-int con 4)
        (setf (pgresult-incomplete result) t)
        (setf (pgresult-tuples result) (nreverse tuples))
        (setf (pgresult-status result) "SUSPENDED")
        (pg-connection-set-busy con nil)
        (cl-return-from pg-fetch result))

       ;; CommandComplete -- one SQL command has completed (portal's execution is completed)
       (?C
        (let* ((msglen (pg--read-net-int con 4))
               (msg (pg--read-chars con (- msglen 5)))
               (_null (pg--read-char con)))
          (setf (pgresult-status result) msg))
        (setf (pgresult-incomplete result) nil)
        (when (> max-rows 0)
          ;; send a Sync message to close the portal and request the ReadyForQuery
          (pg--send-char con ?S)
          (pg--send-uint con 4 4)
          (pg-flush con)))

       ;; EmptyQueryResponse -- the response to an empty query string
       (?I
        (pg--read-net-int con 4)
        (setf (pgresult-status result) "EMPTY")
        (setf (pgresult-incomplete result) nil))

       ;; NoData message
       (?n
        (pg--read-net-int con 4))

       ;; ErrorResponse
       (?E
        (pg-handle-error-response con))

       ;; NoticeResponse
       (?N
        (let ((notice (pg-read-error-response con)))
          (dolist (handler pg-handle-notice-functions)
            (funcall handler notice))))

       ;; CursorResponse
       (?P
        (let ((portal (pg--read-string con)))
          (setf (pgresult-portal result) portal)))

       ;; ReadyForQuery
       (?Z
        (let ((_msglen (pg--read-net-int con 4))
              (status (pg--read-char con)))
          ;; status is 'I' or 'T' or 'E', Idle or InTransaction or Error
          (when (eql ?E status)
            (message "PostgreSQL ReadyForQuery message with error status"))
          (setf (pgresult-tuples result) (nreverse tuples))
          (pg-connection-set-busy con nil)
          (cl-return-from pg-fetch result)))

       (t
        (message "Received unexpected message type %s in pg-fetch" c))))))

;; Do a PARSE/BIND/EXECUTE sequence, using the Extended Query message flow.
;;
;; We are careful here to only send a single Describe message even in the case of a multifetch
;; request (retrieving rows progressively with multiple calls to pg-fetch). The attribute
;; information from the initial Describe message is maintained in the pgresult struct that serves as
;; a handle for the pg-fetch requests.
;;
;; We default to using an empty prepared-statement name and empty portal name because PostgreSQL has
;; a fast path for these queries.
;;
;; The user can also use the extended query protocol at a lower level by calling pg-prepare, pg-bind
;; and pg-fetch explicitly (for example, binding a prepared statement to different values in a
;; loop).
(cl-defun pg-exec-prepared (con query typed-arguments &key (max-rows 0) (portal ""))
  "Execute SQL QUERY using TYPED-ARGUMENTS on database connection CON.
Query can contain numbered parameters ($1, $2 etc.) that are
bound to the values in TYPED-ARGUMENTS, which is a list of the
form \\='((42 . \"int4\") (\"42\" . \"text\")).

This uses PostgreSQL's parse/bind/execute extended query protocol
for prepared statements, which allows parameterized queries to
avoid SQL injection attacks. Returns a pgresult structure that
can be decoded with function `pg-result'. It returns at most
MAX-ROWS rows (a value of zero indicates no limit). If more rows
are available, they can later be retrieved with `pg-fetch'."
  ;; (message "pg-exec-prepared: %s with %s" query typed-arguments)
  (when (pgcon-query-log con)
    (with-current-buffer (pgcon-query-log con)
      (insert query "\n")
      (insert (format "   %s\n" typed-arguments)))
    (when noninteractive
      (message "SQL:> %s %s" query typed-arguments)))
  (pg--trim-connection-buffers con)
  (let* ((argument-types (mapcar #'cdr typed-arguments))
         (ps-name (pg-prepare con query argument-types))
         (portal-name (pg-bind con ps-name typed-arguments :portal portal))
         (result (make-pgresult :connection con :portal portal-name)))
    (pg-describe-portal con portal-name)
    (pg-fetch con result :max-rows max-rows)))

(defun pg-ensure-prepared-statement (con ps-name sql argument-types)
  "Return a prepared-statement named PS-NAME for query SQL and ARGUMENT-TYPES.
Either returns the previously prepared statement saved in the
prepared statement cache of our connection CON, or prepares the
statement using pg-prepare and saves it in the cache."
  (let* ((ps-cache (pgcon-prepared-statement-cache con))
         (maybe-statement (gethash ps-name ps-cache)))
    (or maybe-statement
        (let ((ps (pg-prepare con sql argument-types :name ps-name)))
          (puthash ps-name ps ps-cache)
          ps))))

(defun pg-fetch-prepared (con ps-name typed-arguments)
  "Bind arguments to a prepared-statement and fetch results.
PS-NAME is a previously prepared statement name from `pg-prepare'
or `pg-ensure-prepared-statement'. Calls `pg-bind' to bind the
TYPED-ARGUMENTS then fetches query results from PostgreSQL
connection CON. Preparing a statement once then reusing it
multiple times with different argument values allows you avoid
the overhead of sending and parsing the SQL query and calculating
the query plan."
  (let* ((portal-name (pg-bind con ps-name typed-arguments :portal "pgmacs"))
         (result (make-pgresult :connection con :portal portal-name)))
    (pg-describe-portal con portal-name)
    (prog1
        (pg-fetch con result)
      (pg-close-portal con portal-name))))

(cl-defun pg-close-portal (con portal)
  "Close the portal named PORTAL that was opened by `pg-exec-prepared'.
Uses PostgreSQL connection CON."
  (let ((len (+ 4 1 (1+ (length portal)))))
    ;; send a Close message
    (pg--send-char con ?C)
    (pg--send-uint con len 4)
    (pg--send-char con ?P)
    (pg--send-string con portal)
    ;; send a Sync message
    (pg--send-char con ?S)
    (pg--send-uint con 4 4)
    (pg-flush con)
    (cl-loop
     for c = (pg--read-char con) do
     (cl-case c
       ;; ParseComplete
       (?1
        (pg--read-net-int con 4))

       ;; CloseComplete
       (?3
        (pg--read-net-int con 4))

       ;; PortalSuspended: sent by some old PostgreSQL versions here?
       (?s
        (pg--read-net-int con 4))

       ;; ErrorResponse
       (?E
        (pg-handle-error-response con))

       ;; NoticeResponse
       (?N
        (let ((notice (pg-read-error-response con)))
          (dolist (handler pg-handle-notice-functions)
            (funcall handler notice))))

       ;; ReadyForQuery
       (?Z
        (let ((_msglen (pg--read-net-int con 4))
              (status (pg--read-char con)))
          ;; status is 'I' or 'T' or 'E'
          (when (eql ?E status)
            (message "PostgreSQL ReadyForQuery message with error status"))
          (cl-return-from pg-close-portal nil)))

       (t
        (message "Received unexpected message type %s in pg-close-portal" c))))))


(cl-defun pg-copy-from-buffer (con query buf)
  "Execute COPY FROM STDIN on the contents of BUF, according to QUERY.
Uses PostgreSQL connection CON. Returns a result structure which
can be decoded using `pg-result'."
  (unless (string-equal "COPY" (upcase (cl-subseq query 0 4)))
    (signal 'pg-programming-error (list "Invalid COPY query")))
  (unless (cl-search "FROM STDIN" query)
    (signal 'pg-programming-error (list "COPY command must contain 'FROM STDIN'")))
  (pg-connection-set-busy con t)
  (let ((result (make-pgresult :connection con))
        (ce (pgcon-client-encoding con))
        (len (length query)))
    (when (> len (expt 2 32))
      (signal 'pg-user-error (list "Query is too large")))
    (pg--send-char con ?Q)
    (pg--send-uint con (+ 4 len 1) 4)
    (pg--send-string con query)
    (pg-flush con)
    (let ((more-pending t))
      (while more-pending
        (let ((c (pg--read-char con)))
          (cl-case c
            (?G
             ;; CopyInResponse
             (let ((_msglen (pg--read-net-int con 4))
                   (status (pg--read-net-int con 1))
                   (cols (pg--read-net-int con 2))
                   (format-codes (list)))
               ;; status=0, which will be returned by recent backend versions: the backend is
               ;; expecting data in textual format (rows separated by newlines, columns separated by
               ;; separator characters, etc.).
               ;;
               ;; status=1: the backend is expecting binary format (which is similar to DataRow
               ;; format, and which we don't implement here).
               (dotimes (_c cols)
                 (push (pg--read-net-int con 2) format-codes))
               (unless (zerop status)
                 (signal 'pg-error (list "BINARY format for COPY is not implemented")))
               (setq more-pending nil)))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg--read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg--read-int con 4))
                    (channel (pg--read-string con))
                    (payload (pg--read-string con))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg--read-net-int con 4))
                    (msg (pg--read-chars con (- msglen 4)))
                    (items (split-string msg (unibyte-string 0))))
               (when (> (length (cl-first items)) 0)
                 (dolist (handler pg-parameter-change-functions)
                   (funcall handler con (cl-first items) (cl-second items))))))

            (t
             (let ((msg (format "Unknown response type from backend in copy-from-buffer: %s" c)))
               (signal 'pg-protocol-error (list msg))))))))
    ;; Send the input buffer in chunks 1000 lines long.
    (save-excursion
      (with-current-buffer buf
        (goto-char (point-min))
        (while (not (eobp))
          (let* ((chunk-start (point))
                 (chunk-end (progn (dotimes (_ 1000)
                                     (end-of-line)
                                     (unless (eobp) (forward-char)))
                                   (point)))
                 (data (buffer-substring-no-properties chunk-start chunk-end))
                 (encoded (if ce (encode-coding-string data ce t) data)))
            ;; a CopyData message with the encoded data
            (pg--send-char con ?d)
            (pg--send-uint con (+ 4 (length encoded)) 4)
            (pg--send-octets con encoded)
            (pg-flush con)))))
    ;; send a CopyDone message
    (pg--send-char con ?c)
    (pg--send-uint con 4 4)
    (pg-flush con)
    ;; Backend sends us either CopyDone or CopyFail, followed by CommandComplete + ReadyForQuery
    (cl-loop
     for c = (pg--read-char con) do
     (cl-case c
       (?c
        ;; CopyDone
        (let ((_msglen (pg--read-net-int con 4)))
          nil))

       ;; CopyFail
       (?f
        (let* ((msglen (pg--read-net-int con 4))
               (msg (pg--read-chars con (- msglen 4)))
               (emsg (format "COPY failed: %s" msg)))
          (signal 'pg-copy-failed (list emsg))))

       ;; CommandComplete -- SQL command has completed. After this we expect a ReadyForQuery message.
       (?C
        (let* ((msglen (pg--read-net-int con 4))
               (msg (pg--read-chars con (- msglen 5)))
               (_null (pg--read-char con)))
          (setf (pgresult-status result) msg)))

       ;; NotificationResponse
       (?A
        (let* ((_msglen (pg--read-net-int con 4))
               ;; PID of the notifying backend
               (_pid (pg--read-int con 4))
               (channel (pg--read-string con))
               (payload (pg--read-string con))
               (buf (process-buffer (pgcon-process con)))
               (handlers (with-current-buffer buf pgcon--notification-handlers)))
          (dolist (handler handlers)
            (funcall handler channel payload))))

       ;; ErrorResponse
       (?E
        (pg-handle-error-response con))

       ;; ReadyForQuery message
       (?Z
        (let ((_msglen (pg--read-net-int con 4))
              (status (pg--read-char con)))
          (when (eql ?E status)
            (message "PostgreSQL ReadyForQuery message with error status"))
          (pg--trim-connection-buffers con)
          (pg-connection-set-busy con nil)
          (cl-return-from pg-copy-from-buffer result)))

       (t
        (let ((msg (format "Unknown response type from backend in copy-from-buffer/2: %s" c)))
          (signal 'pg-protocol-error (list msg))))))))

;; https://www.postgresql.org/docs/current/sql-copy.html
;; and https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
(cl-defun pg-copy-to-buffer (con query buf)
  "Execute COPY TO STDOUT on QUERY into the buffer BUF.
Uses PostgreSQL connection CON. Returns a result structure which
can be decoded using `pg-result', but with data in BUF."
  (unless (string-equal "COPY" (upcase (cl-subseq query 0 4)))
    (signal 'pg-programming-error (list "Invalid COPY query")))
  (unless (cl-search "TO STDOUT" query)
    (signal 'pg-programming-error (list "COPY command must contain 'TO STDOUT'")))
  (pg-connection-set-busy con t)
  (let ((result (make-pgresult :connection con)))
    (pg--send-char con ?Q)
    (pg--send-uint con (+ 4 (length query) 1) 4)
    (pg--send-string con query)
    (pg-flush con)
    (let ((more-pending t))
      (while more-pending
        (let ((c (pg--read-char con)))
          (cl-case c
            ;; CopyOutResponse
            (?H
             (let ((_msglen (pg--read-net-int con 4))
                   (status (pg--read-net-int con 1))
                   (cols (pg--read-net-int con 2))
                   (format-codes (list)))
               ;; status=0 indicates the overall COPY format is textual (rows separated by
               ;; newlines, columns separated by separator characters, etc.). 1 indicates the
               ;; overall copy format is binary (which we don't implement here).
               (dotimes (_c cols)
                 (push (pg--read-net-int con 2) format-codes))
               (unless (zerop status)
                 (signal 'pg-error (list "BINARY format for COPY is not implemented")))
               (setq more-pending nil)))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg--read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg--read-int con 4))
                    (channel (pg--read-string con))
                    (payload (pg--read-string con))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg--read-net-int con 4))
                    (msg (pg--read-chars con (- msglen 4)))
                    (items (split-string msg (unibyte-string 0))))
               (when (> (length (cl-first items)) 0)
                 (dolist (handler pg-parameter-change-functions)
                   (funcall handler con (cl-first items) (cl-second items))))))

            (t
             (let ((msg (format "Unknown response type from backend in copy-to-buffer: %s" c)))
               (signal 'pg-protocol-error (list msg))))))))
    ;; Backend sends us CopyData, CopyDone or CopyFail, followed by CommandComplete + ReadyForQuery
    (with-current-buffer buf
      ;; TODO: set the buffer to CSV mode?
      (cl-loop
       for c = (pg--read-char con) do
       (cl-case c
         ;; CopyData
         (?d
          (let* ((msglen (pg--read-net-int con 4))
                 (payload (pg--read-chars-old con (- msglen 4)))
                 (ce (pgcon-client-encoding con))
                 (decoded (if ce (decode-coding-string payload ce t) payload)))
            (insert decoded)))

         ;; CopyDone
         (?c
          (let ((_msglen (pg--read-net-int con 4)))
            nil))

         ;; CopyFail
         (?f
          (let* ((msglen (pg--read-net-int con 4))
                 (msg (pg--read-chars con (- msglen 4)))
                 (emsg (format "COPY failed: %s" msg)))
            (signal 'pg-copy-failed (list emsg))))

         ;; CommandComplete -- SQL command has completed. After this we expect a ReadyForQuery message.
         (?C
          (let* ((msglen (pg--read-net-int con 4))
                 (msg (pg--read-chars con (- msglen 5)))
                 (_null (pg--read-char con)))
            (setf (pgresult-status result) msg)))

         ;; NotificationResponse
         (?A
          (let* ((_msglen (pg--read-net-int con 4))
                 ;; PID of the notifying backend
                 (_pid (pg--read-int con 4))
                 (channel (pg--read-string con))
                 (payload (pg--read-string con))
                 (buf (process-buffer (pgcon-process con)))
                 (handlers (with-current-buffer buf pgcon--notification-handlers)))
            (dolist (handler handlers)
              (funcall handler channel payload))))

         ;; ErrorResponse
         (?E
          (pg-handle-error-response con))

         ;; ReadyForQuery message
         (?Z
          (let ((_msglen (pg--read-net-int con 4))
                (status (pg--read-char con)))
            (when (eql ?E status)
              (message "PostgreSQL ReadyForQuery message with error status"))
            (pg--trim-connection-buffers con)
            (pg-connection-set-busy con nil)
            (cl-return-from pg-copy-to-buffer result)))

         (t
          (let ((msg (format "Unknown response type from backend in copy-to-buffer/2: %s" c)))
            (signal 'pg-protocol-error (list msg)))))))))


(cl-defun pg-sync (con)
  (pg-connection-set-busy con t)
  ;; discard any content in our process buffer
  (with-current-buffer (process-buffer (pgcon-process con))
    (setq-local pgcon--position (point-max)))
  (when (member (pgcon-server-variant con) '(datafusion))
    (cl-return-from pg-sync nil))
  (pg--send-char con ?S)
  (pg--send-uint con 4 4)
  (pg-flush con)
  (when (fboundp 'thread-yield)
    (thread-yield))
  ;; Read the ReadyForQuery message
  (cl-loop
   for c = (pg--read-char con) do
   (cl-case c
     ;; ErrorResponse
     (?E
      (pg-handle-error-response con))

     ;; NoData
     (?n
      (pg--read-net-int con 4))

     ;; ParseComplete
     (?1
      (pg--read-net-int con 4))

     ;; BindComplete
     (?2
      (pg--read-net-int con 4))

     ;; CloseComplete
     (?3
      (pg--read-net-int con 4))

     ;; RowDescription message. We really shouldn't be seeing this here, but some PostgreSQL
     ;; variants like TheNile are sending this after the Sync. Read and discard the attributes.
     (?T
      (pg--read-attributes con))

     ;; DataRow. Should not be seen here; read and discard.
     (?D
      (let ((msglen (pg--read-net-int con 4)))
        (pg--read-chars con (- msglen 4))))

     ;; CommandComplete -- read and discard
     (?C
      (let ((msglen (pg--read-net-int con 4)))
        (pg--read-chars con (- msglen 4))))

     ;; ReadyForQuery message
     (?Z
      (let ((_msglen (pg--read-net-int con 4))
            (status (pg--read-char con)))
        (when (eql ?E status)
          (message "PostgreSQL ReadyForQuery message with error status"))
        (pg-connection-set-busy con nil)
        (cl-return-from pg-sync nil)))

     (t
      (message "Unexpected message type after Sync: %s" c)
      (pg--unread-char con)
      (pg-connection-set-busy con nil)
      (cl-return-from pg-sync nil)))))


(defun pg-cancel (con)
  "Cancel the command currently being processed by the backend.
The cancellation request concerns the command requested over connection CON."
  ;; Send a CancelRequest message. We open a new connection to the server and
  ;; send the CancelRequest message, rather than the StartupMessage message that
  ;; would ordinarily be sent across a new connection. The server will process
  ;; this request and then close the connection.
  ;;
  ;; We could instead call the function pg_cancel_backend(pid).
  (let* ((ci (pgcon-connect-info con))
         (ccon (cl-case (car ci)
                 ;; :tcp host port dbname user password
                 (:tcp
                  (let* ((buf (generate-new-buffer " *PostgreSQL-cancellation*"))
                         (host (nth 1 ci))
                         (port (nth 2 ci))
                         (process (open-network-stream "postgres-cancel" buf host port :coding nil))
                         (connection (make-pgcon :process process)))
                    (with-current-buffer buf
                      (set-process-coding-system process 'binary 'binary)
                      (set-buffer-multibyte nil)
                      (setq-local pgcon--position 1)
                      (setq-local pgcon--busy t)
                      (setq-local pgcon--notification-handlers (list)))
                    connection))
                 ;; :local path dbname user password
                 (:local
                  (let* ((buf (generate-new-buffer " *PostgreSQL-cancellation*"))
                         (path (nth 1 ci))
                         (process (make-network-process :name "postgres"
                                                        :buffer buf
                                                        :family 'local
                                                        :service path
                                                        :coding nil))
                         (connection (make-pgcon :process process)))
                    (with-current-buffer buf
                      (set-process-coding-system process 'binary 'binary)
                      (set-buffer-multibyte nil)
                      (setq-local pgcon--position 1)
                      (setq-local pgcon--busy t)
                      (setq-local pgcon--notification-handlers (list)))
                    connection)))))
    (setf (pgcon-output-buffer ccon)
          (generate-new-buffer " *PostgreSQL output buffer*"))
    (with-current-buffer (pgcon-output-buffer ccon)
      (set-buffer-multibyte nil)
      (buffer-disable-undo)
      (setq-local pgcon--position 1))
    (pg--send-uint ccon 16 4)
    (pg--send-uint ccon 80877102 4)
    (pg--send-uint ccon (pgcon-pid con) 4)
    ;; From PostgreSQL v18 and version 3.2 of the wire protocol, the "cancel request keys" can
    ;; be variable in length, up to 256 bits. Previously, they were only 32 bits in length.
    (pg--send-octets ccon (pgcon-secret con))
    (pg-flush ccon)
    (pg-disconnect ccon)))

(defun pg-disconnect (con)
  "Close the PostgreSQL connection CON.
This command should be used when you have finished with the database.
It will release memory used to buffer the data transfered between
PostgreSQL and Emacs. CON should no longer be used."
  ;; send a Terminate message
  (pg-connection-set-busy con t)
  ;; If we use the immediate-output mode of sending data to PostgreSQL (instead of the buffer-output
  ;; mode), PostgreSQL can close the network connection before we have finished flushing the output.
  ;; This triggers an Emacs error, which we don't want to propagate to the caller here.
  (ignore-errors
    (pg--send-char con ?X)
    (pg--send-uint con 4 4)
    (pg-flush con))
  (let ((process (pgcon-process con)))
    (delete-process process)
    (kill-buffer (process-buffer process))
    (kill-buffer (pgcon-output-buffer con)))
  (when (pgcon-query-log con)
    (kill-buffer (pgcon-query-log con)))
  (clrhash (pgcon-parser-by-oid con))
  (clrhash (pgcon-typname-by-oid con))
  (clrhash (pgcon-oid-by-typname con))
  nil)



;; type coercion support ==============================================
;;
;; When returning data from a SELECT statement, PostgreSQL starts by
;; sending some metadata describing the attributes. This information
;; is read by `pg-read-attributes', and consists of each attribute's
;; name (as a string), its size (in bytes), and its type (as an oid
;; which identifies a row in the PostgreSQL system table pg_type). Each
;; row in pg_type includes the type's name (as a string).
;;
;; We are able to parse a certain number of the PostgreSQL types (for
;; example, numeric data is converted to a numeric Emacs Lisp type,
;; dates are converted to the Emacs date representation, booleans to
;; Emacs Lisp booleans). However, there isn't a fixed mapping from a
;; type to its OID which is guaranteed to be stable across database
;; installations, so we need to build a table mapping OIDs to parser
;; functions.
;;
;; This is done by the procedure `pg-initialize-parsers', which is run
;; the first time a connection is initiated with the database from
;; this invocation of Emacs, and which issues a SELECT statement to
;; extract the required information from pg_type. This initialization
;; imposes a slight overhead on the first request, which you can avoid
;; by setting `pg-disable-type-coercion' to non-nil if it bothers you.
;;
;; see `man pgbuiltin' for details on PostgreSQL builtin types. Also see
;; https://www.npgsql.org/dev/types.html for useful information on the wire format for various
;; types.

;; This function is generally called upon establishing a connection to PostgreSQL. It may also be
;; called later when we encounter an OID that is not present in the cache, indicating that some
;; other activity has led to the creation of new PostgreSQL types (e.g. "CREATE TYPE ..."), and that
;; we need to repopulate our caches.
;;
;; Some databases such as Clickhouse that implement the PostgreSQL wire protocol do not implement the
;; pg_type table. They send all data in textual format with an OID of zero. For this reason, we
;; tolerate an error in the query on pg_type and leave all our oid-related caches empty.
;;
;; Note: the psycopg libary makes the following query to also retrieve datatype delimiters and array
;; types:
;;
;; SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter
;; FROM pg_type t
;; WHERE t.oid = to_regtype($1)
;; ORDER BY t.oid

(cl-defun pg-initialize-parsers (con)
  "Initialize the datatype parsers on PostgreSQL connection CON."
  (when (eq 'clickhouse (pgcon-server-variant con))
    (cl-return-from pg-initialize-parsers nil))
  (let ((type-names (list))
        (parser-by-oid (pgcon-parser-by-oid con))
        (oid-by-typname (pgcon-oid-by-typname con))
        (typname-by-oid (pgcon-typname-by-oid con)))
    (clrhash parser-by-oid)
    (clrhash typname-by-oid)
    (clrhash oid-by-typname)
    (maphash (lambda (k _v) (cl-pushnew k type-names :test #'string=)) pg--serializers)
    (maphash (lambda (k _v) (cl-pushnew k type-names :test #'string=)) pg--textual-serializers)
    (maphash (lambda (k _v) (cl-pushnew k type-names :test #'string=)) pg--parser-by-typname)
    (let* ((qnames (mapcar (lambda (tn) (format "'%s'" tn)) type-names))
           (pg-type
            (pcase (pgcon-server-variant con)
              ('stoolap "pg_type")
              (_ "pg_catalog.pg_type")))
           (sql (format "SELECT typname,oid FROM %s WHERE typname IN (%s)"
                        pg-type (string-join qnames ",")))
           (res (ignore-errors (pg-exec con sql)))
           (pgtypes (and res (pg-result res :tuples)))
           ;; We only use the pg_type information if it looks plausible, and otherwise populate our
           ;; oid<->typname mappings with some predefined data (though strictly speaking there is no
           ;; guarantee that this internal information will remain the same in future PostgreSQL
           ;; releases, it is unlikely to change). This is a workaround for databases like
           ;; GreptimeDB that populate pg_types with invalid information like ("UInt8" "7").
           (rows (if (cl-position '("oid" "26") pgtypes :test #'equal)
                     pgtypes
                   '(("bool" "16")
                     ("bytea" "17")
                     ("char" "18")
                     ("name" "19")
                     ("int8" "20")
                     ("int2" "21")
                     ("int4" "23")
                     ("smallint" "21")
                     ("integer" "23")
                     ("bigint" "20")
                     ("text" "25")
                     ("oid" "26")
                     ("json" "114")
                     ("xml" "142")
                     ("float4" "700")
                     ("float8" "701")
                     ("bpchar" "1042")
                     ("varchar" "1043")
                     ("date" "1082")
                     ("time" "1083")
                     ("timestamp" "1114")
                     ("timestamptz" "1184")
                     ("bit" "1560")
                     ("varbit" "1562")
                     ("numeric" "1700")
                     ("uuid" "2950")
                     ("jsonb" "3802")
                     ("_xml" "143")
                     ("_json" "199")
                     ("_bool" "1000")
                     ("_char" "1002")
                     ("_bpchar" "1014")
                     ("_varchar" "1015")
                     ("_int8" "1016")
                     ("_int2" "1005")
                     ("_int4" "1007")
                     ("_text" "1009")
                     ("_float4" "1021")
                     ("_float8" "1022")
                     ("_date" "1182")
                     ("_timestamptz" "1185")
                     ("_numeric" "1231")
                     ("_timetz" "1270")
                     ("_bit" "1561")
                     ("_uuid" "2951")))))
      (dolist (row rows)
        (let* ((typname (cl-first row))
               (oid (cl-parse-integer (cl-second row)))
               (parser (gethash typname pg--parser-by-typname)))
          (puthash typname oid oid-by-typname)
          (puthash oid typname typname-by-oid)
          (when parser
            (puthash oid parser parser-by-oid))))
      ;; Add aliases for types whose canonical name is different from the typname in the pg_type
      ;; table. There does not seem to be a system table that we can query to obtain information on
      ;; these canonical names: they are generated by the function format_type_extended in
      ;; format_type.c but apparently not present in the database in a format that can be queried.
      ;; There is a documented list at https://www.postgresql.org/docs/current/datatype.html
      (let ((aliases '(("smallint" . "int2")
                       ("integer" . "int4")
                       ("int" . "int4")
                       ("bigint" . "int8")
                       ("bit varying" . "bit")
                       ("character" . "char")
                       ("character varying" . "varchar")
                       ("double precision" . "float8")
                       ("float" . "float8")
                       ("numeric" . "decimal")
                       ("real" . "float4"))))
        (cl-loop
         for (alias . typname) in aliases
         for oid = (gethash typname oid-by-typname)
         when oid
         do (puthash alias oid oid-by-typname))))))

(defun pg-parse (con str oid)
  "Deserialize textual representation STR to an Emacs Lisp object.
Uses the client-encoding specified in the connection to PostgreSQL CON."
  (if pg-disable-type-coercion
      str
    (let ((parser (gethash oid (pgcon-parser-by-oid con)))
          (ce (pgcon-client-encoding con)))
      (if parser
          (funcall parser str ce)
        str))))

(defun pg-serialize (object type-name encoding)
  (let ((serializer (gethash type-name pg--serializers)))
    (if serializer
        (funcall serializer object encoding)
      object)))


;; Map between PostgreSQL names for encodings and their Emacs name. See the list at
;;    https://www.postgresql.org/docs/current/multibyte.html
;;
;; For Emacs, see coding-system-alist.
(defconst pg--encoding-names
  `(("UTF8"    . utf-8)
    ("UTF-8"   . utf-8)
    ("UNICODE" . utf-8)
    ("LATIN1"  . ,(coding-system-base 'latin-1))
    ("LATIN2"  . ,(coding-system-base 'latin-2))
    ("LATIN3"  . ,(coding-system-base 'latin-3))
    ("LATIN4"  . ,(coding-system-base 'latin-4))
    ("LATIN5"  . ,(coding-system-base 'latin-5))
    ("LATIN6"  . ,(coding-system-base 'latin-6))
    ("LATIN7"  . ,(coding-system-base 'latin-7))
    ("LATIN8"  . ,(coding-system-base 'latin-8))
    ("LATIN9"  . ,(coding-system-base 'latin-9))
    ("LATIN10" . ,(coding-system-base 'latin-10))
    ("WIN1250" . windows-1250)
    ("WIN1251" . windows-1251)
    ("WIN1252" . windows-1252)
    ("WIN1253" . windows-1253)
    ("WIN1254" . windows-1254)
    ("WIN1255" . windows-1255)
    ("WIN1256" . windows-1256)
    ("WIN1257" . windows-1257)
    ("WIN1258" . windows-1258)
    ("SHIFT_JIS_2004" . ,(coding-system-base 'shift_jis-2004))
    ("SJIS"    . ,(coding-system-base 'shift_jis-2004))
    ("GB18030" . ,(coding-system-base 'gb18030))
    ("EUC_TW"  . ,(coding-system-base 'euc-taiwan))
    ("EUC_KR"  . ,(coding-system-base 'euc-korea))
    ("EUC_JP"  . ,(coding-system-base 'euc-japan))
    ("EUC_CN"  . ,(coding-system-base 'euc-china))
    ("BIG5"    . ,(coding-system-base 'big5))
    ("SQL_ASCII" . ,(coding-system-base 'ascii))))

(defun pg-normalize-encoding-name (name)
  "Convert PostgreSQL encoding NAME to an Emacs encoding name."
  (if (fboundp 'string-equal-ignore-case)
      (cdr (assoc name pg--encoding-names #'string-equal-ignore-case))
    (cdr (assoc name pg--encoding-names #'string-equal))))

;; Note: this set_config() function call does not work on all variants; for example it fails on QuestDB.
(defun pg-set-client-encoding (con encoding)
  "Change the encoding used by the client to ENCODING.
ENCODING should be a string of the form \"UTF8\" or \"LATIN1\" (see
`pg--encoding-names' for all values supported by PostgreSQL). Sends the
SQL command to change the value of the client_encoding runtime
configuration parameter and also modifies the pgcon-client-encoding for
the PostgreSQL connection CON."
  (let ((emacs-encoding-name (pg-normalize-encoding-name encoding)))
    (unless emacs-encoding-name
      (signal 'pg-encoding-error (list (format "Unknown encoding %s" encoding))))
    (pcase (pgcon-server-variant con)
      ('octodb
       (let* ((res (pg-exec con (format "SET client_encoding TO '%s'" encoding)))
              (status (pg-result res :status)))
         (unless (string= "SET" status)
           (signal 'pg-error (list (format "Couldn't set client_encoding to %s" encoding))))))
      (_
       (let* ((res (pg-exec-prepared con "SELECT set_config('client_encoding', $1, false)"
                                     `((,encoding . "text"))))
              (status (pg-result res :status)))
         (unless (string= "SELECT 1" status)
           (signal 'pg-error (list (format "Couldn't set client_encoding to %s" encoding)))))))
    (setf (pgcon-client-encoding con) emacs-encoding-name)))

;; Note that if you register a parser for a new type-name after a PostgreSQL connection has been
;; established, you must call (pg-initialize-parsers *connection*) to hook the parser into the
;; deserialization machinery (this will look up the OID for the new type).
(defun pg-register-parser (type-name parser)
  (puthash type-name parser pg--parser-by-typname))
(put 'pg-register-parser 'lisp-indent-function 'defun)

(defun pg-lookup-parser (type-name)
  (gethash type-name pg--parser-by-typname))

(defun pg-bool-parser (str _encoding)
  (cond ((string= "t" str) t)
        ;; This syntax used by ArcadeDB
        ((string= "true" str) t)
        ((string= "f" str) nil)
        ((string= "false" str) nil)
        ((string= "NULL" str) pg-null-marker)
        (t (let ((msg (format "Badly formed boolean from backend: %s" str)))
             (signal 'pg-protocol-error (list msg))))))

(pg-register-parser "bool" #'pg-bool-parser)

(defun pg-bit-parser (str _encoding)
  "Parse STR as a PostgreSQL bit to an Emacs bool-vector."
  (declare (speed 3))
  (if (string= "NULL" str)
      pg-null-marker
    (let* ((len (length str))
           (bv (make-bool-vector len t)))
      (dotimes (i len)
        (setf (aref bv i) (eql ?1 (aref str i))))
      bv)))

(pg-register-parser "bit" #'pg-bit-parser)
(pg-register-parser "varbit" #'pg-bit-parser)

(defun pg-text-parser (str encoding)
  "Parse PostgreSQL value STR as text using ENCODING."
  (if encoding
      (if (string= "NULL" str)
          pg-null-marker
        (decode-coding-string str encoding))
    str))

;; STR could be either a single character (char datatype) or a character sequence for the PostgreSQL
;; character(n) datatype specifier, which is fixed-length and blank-padded. Note that we return
;; either a single character or a string.
(defun pg-char-parser (str encoding)
  (let ((len (length str)))
    (cond ((zerop len)
           (signal 'pg-protocol-error (list "Unexpected zero-length char data")))
          ((eql 1 len)
           (if encoding
               (aref (pg-text-parser str encoding) 0)
             (aref str 0)))
          (t
           (pg-text-parser str encoding)))))

(pg-register-parser "char" #'pg-char-parser)
(pg-register-parser "bpchar" #'pg-char-parser)
(pg-register-parser "name" #'pg-text-parser)
(pg-register-parser "text" #'pg-text-parser)
(pg-register-parser "varchar" #'pg-text-parser)
(pg-register-parser "xml" #'pg-text-parser)

;; TODO; could verify the UUID syntax here, but it seems unnecessary to double guess PostgreSQL.
(pg-register-parser "uuid" #'pg-text-parser)

(pg-register-parser "bytea"
  ;; BYTEA binary strings (sequence of octets), that use hex escapes. Note
  ;; PostgreSQL setting variable bytea_output which selects between hex escape
  ;; format (the default in recent version) and traditional escape format. We
  ;; assume that hex format is selected.
  ;;
  ;; https://www.postgresql.org/docs/current/datatype-binary.html
  (lambda (str _encoding)
    "Parse PostgreSQL value STR as a binary string using hex escapes."
    (unless (and (eql 92 (aref str 0))   ; \ character
                 (eql ?x (aref str 1)))
      (signal 'pg-protocol-error
              (list "Unexpected format for BYTEA binary string")))
    (decode-hex-string (substring str 2))))

(declare-function json-read-from-string "json.el")

;; We use either the native libjansson support compiled into Emacs, or fall back to the routines
;; from the JSON library. Note however that these do not parse JSON in exactly the same way (in
;; particular, NULL, false and the empty array are handled differently).
(defun pg-json-parser (str _encoding)
  "Parse PostgreSQL value STR as JSON."
  (if (and (fboundp 'json-parse-string)
           (fboundp 'json-available-p)
           (json-available-p))
      ;; Use the JSON support natively compiled into Emacs
      (json-parse-string str)
    ;; Use the parsing routines from the json library
    (require 'json)
    (json-read-from-string str)))

(pg-register-parser "json" #'pg-json-parser)
(pg-register-parser "jsonb" #'pg-json-parser)

(pg-register-parser "jsonpath" #'pg-text-parser)

;; This function must be called before using the HSTORE extension. It loads the extension if
;; necessary, and sets up the parsing support for HSTORE datatypes. This is necessary because
;; the hstore type is not defined on startup in the pg_type table.
;;
;; https://www.postgresql.org/docs/current/hstore.html
(defun pg-hstore-setup (con)
  "Prepare for use of HSTORE datatypes on PostgreSQL connection CON.
Return nil if the extension could not be loaded."
  (when (condition-case nil
            (pg-exec con "CREATE EXTENSION IF NOT EXISTS hstore")
          (pg-error nil))
    (let* ((res (pg-exec con "SELECT oid FROM pg_catalog.pg_type WHERE typname='hstore'"))
           (oid (car (pg-result res :tuple 0)))
           (parser (pg-lookup-parser "hstore"))
           (parser-by-oid (pgcon-parser-by-oid con))
           (oid-by-typname (pgcon-oid-by-typname con)))
      (when parser
        (puthash oid parser parser-by-oid))
      (puthash "hstore" oid oid-by-typname))
    (pg-register-textual-serializer "hstore"
      (lambda (ht encoding)
        (unless (hash-table-p ht)
          (pg-signal-type-error "Expecting a hash-table, got %s" ht))
        (let ((kv (list)))
          ;; FIXME should escape \" characters in k and v
          (maphash (lambda (k v) (push (format "\"%s\"=>\"%s\""
                                          (pg--serialize-text k encoding)
                                          (pg--serialize-text v encoding)) kv))
                   ht)
          (string-join kv ","))))))

;; Note however that the hstore type is generally not present in the pg_type table
;; upon startup, so we need to call `pg-hstore-setup' before using HSTORE datatypes.
(pg-register-parser "hstore"
  ;; We receive something like "\"a\"=>\"1\", \"b\"=>\"2\""
  (lambda (str encoding)
    "Parse PostgreSQL value STR as HSTORE content."
    (cl-flet ((parse (v)
                (if (string= "NULL" v)
                    nil
                  (unless (and (eql ?\" (aref v 0))
                               (eql ?\" (aref v (1- (length v)))))
                    (signal 'pg-protocol-error '("Unexpected format for HSTORE content")))
                  (pg-text-parser (substring v 1 (1- (length v))) encoding))))
      (let ((hstore (make-hash-table :test #'equal)))
        (dolist (segment (split-string str "," t "\s+"))
          (let* ((kv (split-string segment "=>" t "\s+")))
            (puthash (parse (car kv)) (parse (cadr kv)) hstore)))
        hstore))))

(defun pg-number-parser (str _encoding)
  "Parse PostgreSQL value STR as a number."
  (cl-parse-integer str))

(pg-register-parser "count" #'pg-number-parser)
(pg-register-parser "smallint" #'pg-number-parser)
(pg-register-parser "integer" #'pg-number-parser)
(pg-register-parser "bigint" #'pg-number-parser)
(pg-register-parser "int2" #'pg-number-parser)
(pg-register-parser "int4" #'pg-number-parser)
(pg-register-parser "int8" #'pg-number-parser)
(pg-register-parser "oid" #'pg-number-parser)

;; We need to handle +Inf, -Inf, NaN specially because the Emacs Lisp reader uses a specific format
;; for them.
(defun pg-float-parser (str _encoding)
  "Parse PostgreSQL value STR as a floating-point value."
  (cond ((string= str "Infinity")
         1.0e+INF)
        ((string= str "-Infinity")
         -1.0e+INF)
        ((string= str "NaN")
         0.0e+NaN)
        ((string= str "NULL")
         pg-null-marker)
        (t
         (string-to-number str))))

(pg-register-parser "numeric" #'pg-float-parser)
(pg-register-parser "decimal" #'pg-float-parser)
(pg-register-parser "float" #'pg-float-parser)
(pg-register-parser "float4" #'pg-float-parser)
(pg-register-parser "float8" #'pg-float-parser)

;; FIXME we are not currently handling multidimensional arrays correctly. They are serialized by
;; PostgreSQL using the same typid as a unidimensional array, with only the presence of additional
;; levels of {} marking the extra dimensions.
;; See https://www.postgresql.org/docs/current/arrays.html

(defun pg-intarray-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of integers."
  (cl-flet ((parse-int (str)
              (if (string= "NULL" str)
                  pg-null-marker
                (cl-parse-integer str))))
    (let ((len (length str)))
      (unless (and (eql (aref str 0) ?{)
                   (eql (aref str (1- len)) ?}))
        (signal 'pg-protocol-error (list "Unexpected format for int array")))
      (let ((maybe-items (cl-subseq str 1 (- len 1))))
        (if (zerop (length maybe-items))
            (vector)
          (let ((items (split-string maybe-items ",")))
            (apply #'vector (mapcar #'parse-int items))))))))

(pg-register-parser "_int2" #'pg-intarray-parser)
(pg-register-parser "_int2vector" #'pg-intarray-parser)
(pg-register-parser "_int4" #'pg-intarray-parser)
(pg-register-parser "_int8" #'pg-intarray-parser)

(defun pg-floatarray-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of floats."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for float array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-float-parser x nil)) items)))))))

(pg-register-parser "_float4" #'pg-floatarray-parser)
(pg-register-parser "_float8" #'pg-floatarray-parser)
(pg-register-parser "_numeric" #'pg-floatarray-parser)

(defun pg-boolarray-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of boolean values."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for bool array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-bool-parser x nil)) items)))))))

(pg-register-parser "_bool" #'pg-boolarray-parser)

(defun pg-chararray-parser (str encoding)
  "Parse PostgreSQL value STR as an array of characters using ENCODING."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for char array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-char-parser x encoding)) items)))))))

(pg-register-parser "_char" #'pg-chararray-parser)
(pg-register-parser "_bpchar" #'pg-chararray-parser)

(defun pg-textarray-parser (str encoding)
  "Parse PostgreSQL value STR as an array of TEXT values.
Uses text encoding ENCODING."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for text array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-text-parser x encoding)) items)))))))

(pg-register-parser "_text" #'pg-textarray-parser)
(pg-register-parser "_varchar" #'pg-textarray-parser)

;; Anonymouse records in PostgreSQL (oid = 2249) are little used in practice, and difficult to parse
;; because we receive no information concerning the types of the different record "columns".
;;
;;   SELECT (1,2) --> "(1,2)"
;;   SELECT (null,1,2) --> "(,1,2)"
;;   SELECT ('foo,ble',null) --> "(\"foo,ble\",)"
;;   SELECT (1, (2, 3)) --> "(1,\"(2,3)\")"     -- nested records are allowed
;;   SELECT (1, '(2,3)') --> "(1,\"(2,3)\")"    -- note the ambiguity!
;;
;; We simply return these as an unparsed string.
(pg-register-parser "record" #'pg-text-parser)

;; Something like "[10.4,20)". TODO: handle multirange types (from PostgreSQL v14)
(defun pg-numrange-parser (str _encoding)
  "Parse PostgreSQL value STR as a numerical range."
  (if (string= "empty" str)
      (list :range)
    (let* ((len (length str))
           (lower-type (aref str 0))
           (upper-type (aref str (1- len))))
      (unless (and (cl-find lower-type "[(")
                   (cl-find upper-type ")]"))
        (signal 'pg-protocol-error '("Unexpected format for numerical range")))
      (let* ((segments (split-string (cl-subseq str 1 (1- len)) ","))
             (lower-str (nth 0 segments))
             (upper-str (nth 1 segments))
             ;; if the number is empty, that's a NULL lower or upper bound
             (lower (if (zerop (length lower-str)) nil (string-to-number lower-str)))
             (upper (if (zerop (length upper-str)) nil (string-to-number upper-str))))
        (unless (eql 2 (length segments))
          (signal 'pg-protocol-error '("Unexpected number of elements in numerical range")))
        (list :range lower-type lower upper-type upper)))))

(pg-register-parser "int4range" #'pg-numrange-parser)
(pg-register-parser "int8range"  #'pg-numrange-parser)
(pg-register-parser "numrange" #'pg-numrange-parser)

(pg-register-parser "money" #'pg-text-parser)

(defun pg-uuidarray-parser (str encoding)
  "Parse PostgreSQL value STR as an array of UUID values.
Uses text encoding ENCODING."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for UUID array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-text-parser x encoding)) items)))))))

(pg-register-parser "_uuid" #'pg-uuidarray-parser)

;; format for ISO dates is "1999-10-24"
(defun pg-date-parser (str _encoding)
  "Parse PostgreSQL value STR as a date."
  (if (string= "infinity" str)
      (encode-time (list 0 0 0 1 1 999999))
    (let ((year  (string-to-number (substring str 0 4)))
          (month (string-to-number (substring str 5 7)))
          (day   (string-to-number (substring str 8 10))))
      (encode-time (list 0 0 0 day month year)))))

(pg-register-parser "date" #'pg-date-parser)

(defun pg-datearr-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of date values."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-date-parser x nil)) items)))))))

(pg-register-parser "_date" #'pg-datearr-parser)


(defconst pg--ISODATE_REGEX
  (concat "^\\([0-9]+\\)-\\([0-9][0-9]\\)-\\([0-9][0-9]\\)" ; Y-M-D
          "\\([ T]\\)" ; delim
          "\\([0-9][0-9]\\):\\([0-9][0-9]\\):\\([.0-9]+\\)" ; H:M:S.S
          "\\(?:\\(?:[zZ]\\)\\|\\(?:[-+][0-9]\\{2\\}:?\\(?:[0-9]\\{2\\}\\)?\\)\\)?$")) ; TZ

;;  format for abstime/timestamp etc with ISO output syntax is
;;;    "1999-01-02 14:32:53.789+01"
;; which we convert to the internal Emacs date/time representation
;; (there may be a fractional seconds quantity as well, which the regex
;; handles)
(defun pg-isodate-with-timezone-parser (str _encoding)
  "Parse PostgreSQL value STR as an ISO-formatted date."
  (if (string-match pg--ISODATE_REGEX str)
      (let ((iso (replace-match "T" nil nil str 4)))
        ;; Use of parse-iso8601-time-string with a second argument is only supported from Emacs 29.1
        ;; onwards. In earlier versions we call the function with a single argument, which loses
        ;; sub-second precision (and will fail our test suite for this reason).
        (if (>= emacs-major-version 29)
            (parse-iso8601-time-string iso t)
          (parse-iso8601-time-string iso)))
    (let ((msg (format "Badly formed ISO timestamp from backend: %s" str)))
      (signal 'pg-protocol-error (list msg)))))

(defun pg-isodate-without-timezone-parser (str _encoding)
  "Parse PostgreSQL value STR as an ISO-formatted date."
  (if (string-match pg--ISODATE_REGEX str)
      (let ((year    (string-to-number (match-string 1 str)))
            (month   (string-to-number (match-string 2 str)))
            (day     (string-to-number (match-string 3 str)))
            (hours   (string-to-number (match-string 5 str)))
            (minutes (string-to-number (match-string 6 str)))
            (seconds (string-to-number (match-string 7 str)))
            (tz      nil))
        ;; a tz of nil means that we are parsing into Emacs' local time, which is dependent on the
        ;; setting of the TZ environment variable.
        (encode-time (list seconds minutes hours day month year nil -1 tz)))
    (let ((msg (format "Badly formed ISO timestamp from backend: %s" str)))
      (signal 'pg-protocol-error (list msg)))))

(pg-register-parser "timestamp"  #'pg-isodate-without-timezone-parser)
(pg-register-parser "timestamptz" #'pg-isodate-with-timezone-parser)
(pg-register-parser "datetime" #'pg-isodate-with-timezone-parser)

;; FIXME TODO _timestamp _timestamptz _datetime

(pg-register-parser "time" #'pg-text-parser)     ; preparsed "15:32:45"
(pg-register-parser "timetz" #'pg-text-parser)
(pg-register-parser "reltime" #'pg-text-parser)     ; don't know how to parse these
(pg-register-parser "timespan" #'pg-text-parser)
(pg-register-parser "tinterval" #'pg-text-parser)

;; This is usable for time, timespan etc. types that we currently parse as strings.
(defun pg-timearr-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of time or date values."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for array")))
    (let ((maybe-items (cl-subseq str 1 (- len 1))))
      (if (zerop (length maybe-items))
          (vector)
        (let ((items (split-string maybe-items ",")))
          (apply #'vector (mapcar (lambda (x) (pg-text-parser x nil)) items)))))))

(pg-register-parser "_time" #'pg-timearr-parser)
(pg-register-parser "_timetz" 'pg-timearr-parser)
(pg-register-parser "_reltime" 'pg-timearr-parser)
(pg-register-parser "_timespan" 'pg-timearr-parser)
(pg-register-parser "_tinterval" 'pg-timearr-parser)


;; A tsvector is a type used by PostgreSQL to support full-text search in documents.
;;
;; Possible input formats:
;;   "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"
;;   "'    ' 'contains' 'lexeme' 'spaces' 'the'"
;;   "'Joe''s' 'a' 'contains' 'lexeme' 'quote' 'the'"
;;   "'a':1,6,10 'and':8 'ate':9 'cat':3"
;;   "'a':1A 'cat':5 'fat':2B,4C"
;;
;; Parse this to a list of pg-ts.
(cl-defstruct pg-ts
  "A component of a PostgreSQL tsvector, comprising lexeme and its weighted-positions."
  lexeme weighted-positions)

;; https://elpa.gnu.org/packages/peg.html
(defun pg-tsvector-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
        ((tsvector tslist (eol))
         (tslist ts (* (and (+ [space]) ts)))
         (ts lexeme positions
             `(lx wpos -- (make-pg-ts :lexeme lx :weighted-positions wpos)))
         ;; We want 'Joe''s' to be parsed as the lexeme "Joe's"
         (lexeme "'" (substring (or (+ (or [alpha] "''")) (+ [space]))) "'"
                 `(lx -- (string-replace "''" "'" lx)))
         (positions (or (and ":" (list (* (and position ",")) position))
                        `(-- nil)))
         (position (and (substring (+ [digit])) opt-weight)
                   `(pos w -- (cons (string-to-number pos) w)))
         ;; The default weight is :D. It's omitted in the default PostgreSQL string format for a
         ;; tsvector, but it seems better to make it explicit.
         (opt-weight (or weight `(-- (intern ":D"))))
         (weight (substring (or [A-D] [a-d]))
                 `(w -- (intern (concat ":" (upcase w))))))
      (peg-run (peg tsvector)))))

;; (pg-tsvector-parser "'foo'" nil)
;; (pg-tsvector-parser "'foo' 'bar'" nil)
;; (pg-tsvector-parser "'    ' 'contains' 'lexeme' 'spaces' 'the'" nil)
;; (pg-tsvector-parser "'fooé' 'bar':44 'bizzles':1" nil)
;; (pg-tsvector-parser "'foo' 'bar':4 'bizlles':1,2,10" nil)
;; (pg-tsvector-parser "'a':1A 'cat':5 'fat':2B,4C" nil)
;; (pg-tsvector-parser "'Joe''s' 'a' 'contains' 'lexeme' 'quote' 'the'" nil)

(pg-register-parser "tsvector" #'pg-tsvector-parser)

;; TODO: also define a parser for the tsquery type


;;; Support for the pgvector extension (vector similarity search).

;; This function must be called before using the pgvector extension. It loads the extension if
;; necessary, and sets up the parsing support for vector datatypes.
(cl-defun pg-vector-setup (con)
  "Prepare for use of VECTOR datatypes on PostgreSQL connection CON.
Return nil if the extension could not be set up."
  (when (member (pgcon-server-variant con) '(datafusion))
    (cl-return-from pg-vector-setup nil))
  ;; Failure of this CREATE EXTENSION statement does not necessarily mean that the database variant
  ;; does not support the vector datatype (cf. for example CedarDB).
  (condition-case nil
      (pg-exec con "CREATE EXTENSION IF NOT EXISTS vector")
    (pg-error nil))
  (let* ((res (pg-exec con "SELECT oid FROM pg_catalog.pg_type WHERE typname='vector'"))
         (oid (car (pg-result res :tuple 0)))
         (parser (pg-lookup-parser "vector")))
    (when (and parser oid)
      (puthash "vector" oid (pgcon-oid-by-typname con))
      (puthash oid "vector" (pgcon-typname-by-oid con))
      (puthash oid parser (pgcon-parser-by-oid con)))))

;; pgvector embeddings are sent by the database as strings, in the form "[1,2,3]" or ["0.015220831,
;; 0.039211094, 0.02235647]"
(pg-register-parser "vector"
  (lambda (s _e)
    (let ((len (length s)))
      (unless (and (eql (aref s 0) ?\[)
                   (eql (aref s (1- len)) ?\]))
        (signal 'pg-protocol-error (list "Unexpected format for VECTOR embedding")))
      (let ((segments (split-string (cl-subseq s 1 (1- len)) ",")))
        (apply #'vector (mapcar #'string-to-number segments))))))


(defun pg-register-serializer (type-name serializer)
  (puthash type-name serializer pg--serializers))

(defun pg-register-textual-serializer (type-name serializer)
  (puthash type-name serializer pg--serializers)
  (puthash type-name t pg--textual-serializers))
(put 'pg-register-serializer 'lisp-indent-function 'defun)
(put 'pg-register-textual-serializer 'lisp-indent-function 'defun)

(defun pg--serialize-text (object encoding)
  (if encoding
      (encode-coding-string object encoding t)
    object))

(defun pg--serialize-binary (object _encoding)
  object)

(pg-register-serializer "text" #'pg--serialize-text)
(pg-register-serializer "varchar" #'pg--serialize-text)
(pg-register-serializer "xml" #'pg--serialize-text)

(pg-register-serializer "bytea" #'pg--serialize-binary)
(pg-register-serializer "jsonb" #'pg--serialize-binary)
(pg-register-textual-serializer "jsonpath" #'pg--serialize-text)

;; Expected format: e313723b-7d52-4c87-bf0f-b7d73d6284fd
(defun pg--serialize-uuid (uuid _encoding)
  (let ((uuid-rx (rx string-start
                     (group (repeat 8 xdigit)) ?-
                     (group (repeat 4 xdigit)) ?-
                     (group (repeat 4 xdigit)) ?-
                     (group (repeat 4 xdigit)) ?-
                     (group (repeat 12 xdigit))
                     string-end)))
    (unless (string-match uuid-rx uuid)
      (pg-signal-type-error "Expecting a UUID, got %s" uuid)))
  (let* ((hx (concat (match-string 1 uuid)
                     (match-string 2 uuid)
                     (match-string 3 uuid)
                     (match-string 4 uuid)
                     (match-string 5 uuid))))
    (decode-hex-string hx)))

(pg-register-serializer "uuid" #'pg--serialize-uuid)

(pg-register-serializer "bool" (lambda (v _encoding) (if v (unibyte-string 1) (unibyte-string 0))))

(defun pg--serialize-boolvec (bv _encoding)
  (unless (bool-vector-p bv)
    (pg-signal-type-error "Expecting a bool-vector, got %s" bv))
  (let* ((len (length bv))
         (out (make-string len ?0)))
    (dotimes (i len)
      (setf (aref out i)
            (if (aref bv i) ?1 ?0)))
    out))

(pg-register-textual-serializer "bit" #'pg--serialize-boolvec)
(pg-register-textual-serializer "varbit" #'pg--serialize-boolvec)


;; Here we assume that the value is a single character: if serializing a string, use the "text"
;; datatype specifier.
(pg-register-serializer "char"
  (lambda (v _encoding)
    (unless (characterp v)
      (pg-signal-type-error "Expecting a character, got %s" v))
    (unless (<= 0 v 255)
      (pg-signal-type-error "Value %s out of range for CHAR type" v))
    (unibyte-string v)))

;; The value may be either a single character, or a string (PostgreSQL uses the bpchar oid with a
;; length field to represent char(n) fields).
(pg-register-serializer "bpchar"
  (lambda (v encoding)
    (cond ((characterp v)
           (unless (<= 0 v 255)
             (pg-signal-type-error "Value %s out of range for BPCHAR type" v))
           (pg--serialize-text (unibyte-string v) encoding))
          ((stringp v)
           (pg--serialize-text v encoding))
          (t
           (pg-signal-type-error "Expecting a character or a string, got %s" v)))))

;; see https://www.postgresql.org/docs/current/datatype-numeric.html
(defun pg--serialize-int2 (v _encoding)
  (unless (integerp v)
    (pg-signal-type-error "Expecting an integer, got %s" v))
  (unless (<= (- (expt 2 15)) v (expt 2 15))
    (pg-signal-type-error "Value %s out of range for INT2 type" v))
  ;; This use of bindat-type makes us depend on Emacs 28.1, released in April 2022.
  (bindat-pack (bindat-type sint 16 nil) v))

(pg-register-serializer "int2" #'pg--serialize-int2)
(pg-register-serializer "smallint" #'pg--serialize-int2)
(pg-register-serializer "smallserial" #'pg--serialize-int2)

(defun pg--serialize-int4 (v _encoding)
  (unless (integerp v)
    (pg-signal-type-error "Expecting an integer, got %s" v))
  (bindat-pack (bindat-type sint 32 nil) v))

(pg-register-serializer "int4"  #'pg--serialize-int4)
(pg-register-serializer "integer" #'pg--serialize-int4)
(pg-register-serializer "serial" #'pg--serialize-int4)
;; see https://www.postgresql.org/docs/current/datatype-oid.html
(pg-register-serializer "oid" #'pg--serialize-int4)

(defun pg--serialize-int8 (v _encoding)
  (unless (integerp v)
    (pg-signal-type-error "Expecting an integer, got %s" v))
  (bindat-pack (bindat-type sint 64 nil) v))

(pg-register-serializer "int8" #'pg--serialize-int8)
(pg-register-serializer "bigint" #'pg--serialize-int8)
(pg-register-serializer "bigserial" #'pg--serialize-int8)

;; We send floats in text format, because we don't know how to access the binary representation from
;; Emacs Lisp. Here a possible conversion routine and reader, but there is probably no performance
;; benefit to using it.
;;
;; https://lists.gnu.org/archive/html/help-gnu-emacs/2002-10/msg00724.html
;; https://www.emacswiki.org/emacs/read-float.el
(defun pg--serialize-float (number _encoding)
  "Serialize floating point NUMBER to PostgreSQL wire-level text format for floats.
Respects floating-point infinities and NaN."
  (unless (numberp number)
    (pg-signal-type-error "Expecting a number, got %s" number))
  (let ((fl (float number)))
    (cond ((= fl 1.0e+INF) "Infinity")
          ((= fl -1.0e+INF) "-Infinity")
          ((isnan fl) "NaN")
          (t
           (number-to-string fl)))))

(pg-register-textual-serializer "float4" #'pg--serialize-float)
(pg-register-textual-serializer "float8" #'pg--serialize-float)
(pg-register-textual-serializer "numeric" #'pg--serialize-float)

;; FIXME probably we should be encoding this.
(defun pg--serialize-json (json _encoding)
  (if (fboundp 'json-serialize)
      (json-serialize json)
    (require 'json)
    (json-encode json)))

(pg-register-textual-serializer "json" #'pg--serialize-json)
(pg-register-textual-serializer "jsonb" #'pg--serialize-json)

(defun pg--serialize-encoded-time-date (encoded-time _encoding)
  (unless (and (listp encoded-time)
               (integerp (car encoded-time))
               (integerp (cadr encoded-time)))
    (pg-signal-type-error "Expecting an encoded time-date (a . b), got %s" encoded-time))
  (format-time-string "%Y-%m-%d" encoded-time))

(pg-register-textual-serializer "date" #'pg--serialize-encoded-time-date)

;; We parse these into an Emacs Lisp "encoded-time", which is represented as a list of two integers.
;; Serialize them back to an ISO timestamp.
(defun pg--serialize-encoded-time (encoded-time _encoding)
  (unless (and (listp encoded-time)
               (integerp (car encoded-time))
               (or (listp (cdr encoded-time))(integerp (cdr encoded-time))))
    (pg-signal-type-error "Expecting an encoded time-date (a . b), got %s" encoded-time))
  (format-time-string "%Y-%m-%dT%T.%N%z" encoded-time "UTC"))

(pg-register-textual-serializer "timestamp"  #'pg--serialize-encoded-time)
(pg-register-textual-serializer "timestamptz" #'pg--serialize-encoded-time)
(pg-register-textual-serializer "datetime" #'pg--serialize-encoded-time)

;; Serialize an elisp vector of numbers (integers or floats) to a string of the form "[44,55,66]"
(pg-register-textual-serializer "vector"
  (lambda (v _encoding)
    (unless (and (vectorp v)
                 (cl-every #'numberp v))
      (pg-signal-type-error "Expecting a vector of numbers, got %s" v))
    (concat "[" (string-join (mapcar #'prin1-to-string v) ",") "]")))


(pg-register-textual-serializer "_text"
  (lambda (vector encoding)
     (concat "{" (string-join (mapcar (lambda (v) (pg--serialize-text v encoding)) vector) ",") "}")))

;; We currently serialize these in textual format. They could also be serialized in binary form as per
;; https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
;; and https://doxygen.postgresql.org/array_8h_source.html
(defun pg--serialize-intarray (vector _encoding)
  (with-temp-buffer
    (insert "{")
    (cl-loop
     for element across vector
     do (insert (number-to-string element) ","))
    (delete-char -1)                    ; the last comma
    (insert "}")
    (buffer-string)))

(pg-register-textual-serializer "_int2" #'pg--serialize-intarray)
(pg-register-textual-serializer "_int4" #'pg--serialize-intarray)
(pg-register-textual-serializer "_int8" #'pg--serialize-intarray)
(pg-register-textual-serializer "_numeric" #'pg--serialize-intarray)
(pg-register-textual-serializer "_oid" #'pg--serialize-intarray)

;; We don't know how to serialize floating point numbers in binary format in Emacs Lisp, so
;; serialize them in textual form.
(pg-register-textual-serializer "_float4"
  (lambda (vector encoding)
    (concat "{" (string-join (mapcar (lambda (v) (pg--serialize-float v encoding)) vector) ",") "}")))

(pg-register-textual-serializer "_float8"
  (lambda (vector encoding)
     (concat "{" (string-join (mapcar (lambda (v) (pg--serialize-float v encoding)) vector) ",") "}")))

(pg-register-textual-serializer "_uuid"
  (lambda (vector encoding)
    (let ((uuid-rx (rx string-start
                       (group (repeat 8 xdigit)) ?-
                       (group (repeat 4 xdigit)) ?-
                       (group (repeat 4 xdigit)) ?-
                       (group (repeat 4 xdigit)) ?-
                       (group (repeat 12 xdigit))
                       string-end)))
      (with-temp-buffer
        (insert "{")
        (cl-loop
         for uuid across vector
         do (unless (string-match uuid-rx uuid)
              (pg-signal-type-error "Expecting a UUID, got %s" uuid))
         (insert (pg--serialize-text uuid encoding) ","))
        (delete-char -1)                    ; the last comma
        (insert "}")
        (buffer-string)))))


;; pwdhash = md5(password + username).hexdigest()
;; hash = ′md5′ + md5(pwdhash + salt).hexdigest()
(defun pg-do-md5-authentication (con user password)
  "Attempt MD5 authentication with PostgreSQL database over connection CON.
Authenticate as USER with PASSWORD, which is either a string or a
zero-argument function that returns a string."
  (let* ((password-string (if (functionp password)
                              (funcall password)
                            password))
         (salt (pg--read-chars con 4))
         (pwdhash (md5 (concat password-string user)))
         (hash (concat "md5" (md5 (concat pwdhash salt)))))
    (pg--send-char con ?p)
    (pg--send-uint con (+ 5 (length hash)) 4)
    (pg--send-string con hash)
    (pg-flush con)))


;; TODO: implement stringprep for user names and passwords, as per RFC4013.
(defun pg-sasl-prep (string)
  string)


(defun pg-logxor-string (s1 s2)
  "Elementwise XOR of each character of strings S1 and S2."
  (declare (speed 3))
  (let ((len (length s1)))
    (cl-assert (eql len (length s2)))
    (let ((out (make-string len 0 nil)))
      (dotimes (i len)
        (setf (aref out i) (logxor (aref s1 i) (aref s2 i))))
      out)))

;; PBKDF2 is a key derivation function used to reduce vulnerability to brute-force password guessing
;; attempts <https://en.wikipedia.org/wiki/PBKDF2>.
(defun pg-pbkdf2-hash-sha256 (password salt iterations)
  "Return the PBKDF2 hash of PASSWORD using SALT and ITERATIONS."
  (declare (speed 3))
  (let* ((hash (gnutls-hash-mac 'SHA256 (cl-copy-seq password) (concat salt (unibyte-string 0 0 0 1))))
         (result hash))
    (dotimes (_i (1- iterations))
      (setf hash (gnutls-hash-mac 'SHA256 (cl-copy-seq password) hash))
      (setf result (pg-logxor-string result hash)))
    result))

;; Implement PBKDF2 by calling out to the nettle-pbkdf2 application (typically available in the
;; "nettle-bin" package) as a subprocess.
(defun pg-pbkdf2-hash-sha256-nettle (password salt iterations)
  "Return the PBKDF2 hash of PASSWORD using SALT and ITERATIONS, with nettle."
  ;; ITERATIONS is a integer
  ;; the hash function in nettle-pbkdf2 is hard coded to HMAC-SHA256
  (require 'hex-util)
  (with-temp-buffer
    (insert (pg-sasl-prep password))
    (call-process-region
     (point-min) (point-max)
     "nettle-pbkdf2"
     t t
     "--raw" "-i" (format "%d" iterations) "-l" "32" salt)
    ;; delete trailing newline character
    (goto-char (point-max))
    (backward-char 1)
    (when (eql ?\n (char-after))
      (delete-char 1))
    ;; out is in the format 55234f50f7f54f13 9e7f13d4becff1d6 aee3ab80a08cc034 c75e8ba21e43e01b
    (let ((out (delete ?\s (buffer-string))))
      (decode-hex-string out))))


;; use NIL to generate a new client nonce on each authentication attempt (normal practice)
;; or specify a string here to force a particular value for test purposes (compare test vectors)
;; Example value: "rOprNGfwEbeRWgbNEkqO"
(defvar pg--*force-client-nonce* nil)


;; SCRAM authentication methods use a password as a shared secret, which can then be used for mutual
;; authentication in a way that doesn't expose the secret directly to an attacker who might be
;; sniffing the communication.
;;
;; https://www.postgresql.org/docs/15/sasl-authentication.html
;; https://www.rfc-editor.org/rfc/rfc7677
(defun pg-do-scram-sha256-authentication (con user password)
  "Attempt SCRAM-SHA-256 authentication with PostgreSQL over connection CON.
Authenticate as USER with PASSWORD, a string."
  (let* ((mechanism "SCRAM-SHA-256")
         (client-nonce (or pg--*force-client-nonce*
                           (apply #'string (cl-loop for i below 32 collect (+ ?A (random 25))))))
         (client-first (format "n,,n=%s,r=%s" user client-nonce))
         (len-cf (length client-first))
         ;; packet length doesn't include the initial ?p message type indicator
         (len-packet (+ 4 (1+ (length mechanism)) 4 len-cf)))
    ;; send the SASLInitialResponse message
    (pg--send-char con ?p)
    (pg--send-uint con len-packet 4)
    (pg--send-string con mechanism)
    (pg--send-uint con len-cf 4)
    (pg--send-octets con client-first)
    (pg-flush con)
    (let ((c (pg--read-char con)))
      (cl-case c
        (?E
         ;; an ErrorResponse message
         (pg-handle-error-response con "during SASL auth"))

        ;; AuthenticationSASLContinue message, what we are hoping for
        (?R
         (let* ((len (pg--read-net-int con 4))
                (type (pg--read-net-int con 4))
                (server-first-msg (pg--read-chars con (- len 8))))
           (unless (eql type 11)
             (let ((msg (format "Unexpected AuthenticationSASLContinue type %d" type)))
               (signal 'pg-protocol-error (list msg))))
           (let* ((components (split-string server-first-msg ","))
                  (r= (cl-find "r=" components :key (lambda (s) (substring s 0 2)) :test #'string=))
                  (r (substring r= 2))
                  (s= (cl-find "s=" components :key (lambda (s) (substring s 0 2)) :test #'string=))
                  (s (substring s= 2))
                  (salt (base64-decode-string s))
                  (i= (cl-find "i=" components :key (lambda (s) (substring s 0 2)) :test #'string=))
                  (iterations (cl-parse-integer (substring i= 2)))
                  (salted-password (pg-pbkdf2-hash-sha256 password salt iterations))
                  ;; beware: gnutls-hash-mac will zero out its first argument (the "secret")!
                  (client-key (gnutls-hash-mac 'SHA256 (cl-copy-seq salted-password) "Client Key"))
                  (server-key (gnutls-hash-mac 'SHA256 (cl-copy-seq salted-password) "Server Key"))
                  (stored-key (secure-hash 'sha256 client-key nil nil t))
                  (client-first-bare (concat "n=" (pg-sasl-prep user) ",r=" client-nonce))
                  (client-final-bare (concat "c=biws,r=" r))
                  (auth-message (concat client-first-bare "," server-first-msg "," client-final-bare))
                  (client-sig (gnutls-hash-mac 'SHA256 stored-key auth-message))
                  (client-proof (pg-logxor-string client-key client-sig))
                  (server-sig (gnutls-hash-mac 'SHA256 server-key auth-message))
                  (client-final-msg (concat client-final-bare ",p=" (base64-encode-string client-proof t))))
             (when (zerop iterations)
               (let ((msg (format "SCRAM-SHA-256: server supplied invalid iteration count %s" i=)))
                 (signal 'pg-protocol-error (list msg))))
             (unless (string= client-nonce (substring r 0 (length client-nonce)))
               (signal 'pg-protocol-error
                       (list "SASL response doesn't include correct client nonce")))
             ;; we send a SASLResponse message with SCRAM client-final-message as content
             (pg--send-char con ?p)
             (pg--send-uint con (+ 4 (length client-final-msg)) 4)
             (pg--send-octets con client-final-msg)
             (pg-flush con)
             (let ((c (pg--read-char con)))
               (cl-case c
                 (?E
                  ;; an ErrorResponse message
                  (pg-handle-error-response con "after SASLResponse"))

                 (?R
                  ;; an AuthenticationSASLFinal message
                  (let* ((len (pg--read-net-int con 4))
                         (type (pg--read-net-int con 4))
                         (server-final-msg (pg--read-chars con (- len 8))))
                    (unless (eql type 12)
                      (let ((msg (format "Expecting AuthenticationSASLFinal, got type %d" type)))
                        (signal 'pg-protocol-error (list msg))))
                    (when (string= "e=" (substring server-final-msg 0 2))
                      (let ((msg (format "PostgreSQL server error during SASL authentication: %s"
                                         (substring server-final-msg 2))))
                        (signal 'pg-protocol-error (list msg))))
                    (unless (string= "v=" (substring server-final-msg 0 2))
                      (signal 'pg-protocol-error '("Unable to verify PostgreSQL server during SASL auth")))
                    (unless (string= (substring server-final-msg 2)
                                     (base64-encode-string server-sig t))
                      (let ((msg (format "SASL server validation failure: v=%s / %s"
                                         (substring server-final-msg 2)
                                         (base64-encode-string server-sig t))))
                        (signal 'pg-protocol-error (list msg))))
                    ;; should be followed immediately by an AuthenticationOK message
                    )))))))
        (t
         (let ((msg (format "Unexpected response to SASLInitialResponse message: %s" c)))
           (signal 'pg-protocol-error (list msg))))))))

(defun pg-do-sasl-authentication (con user password)
  "Attempt SASL authentication with PostgreSQL over connection CON.
Authenticate as USER with PASSWORD, which is either a string or a
zero-argument function that returns a string."
  (let ((password-string (if (functionp password)
                             (funcall password)
                           password))
        (mechanisms (list)))
    ;; read server's list of preferered authentication mechanisms
    (cl-loop for mech = (pg--read-string con 4096)
             while (not (zerop (length mech)))
             do (push mech mechanisms))
    (if (member "SCRAM-SHA-256" mechanisms)
        (pg-do-scram-sha256-authentication con user password-string)
      (let ((msg (format "Can't handle any of SASL mechanisms %s" mechanisms)))
        (signal 'pg-protocol-error (list msg))))))


(defun pg-table-owner (con table)
  "Return the owner of TABLE in a PostgreSQL database.
TABLE can be a string or a schema-qualified name.
Uses database connection CON."
  (pcase (pgcon-server-variant con)
    ;; QuestDB have a notion of the current user and RBAC, but does not seem to have any information
    ;; on the owner of a particular table.
    ('questdb nil)
    ;; CrateDB does not have information on table owners, but rather on privileges granted on objects to users.
    ('cratedb
     (let* ((sql "SELECT name FROM sys.users WHERE superuser='t'")
            (res (pg-exec con sql))
            (row (pg-result res :tuple 0)))
       (cl-first row)))
    ('vertica
     (let* ((schema (when (pg-qualified-name-p table)
                      (pg-qualified-name-schema table)))
            (table-name (if (pg-qualified-name-p table)
                            (pg-qualified-name-name table)
                          table))
            (schema-sql (if schema " AND table_schema=$2" ""))
            ;; FIXME is this returning a name/string or a referene to the v_catalog.users table?
            ;; https://dataedo.com/kb/query/vertica/list-schemas-in-database
            (sql (concat "SELECT owner_name FROM v_catalog.tables WHERE table_name=$1" schema-sql))
            (args (if schema
                      `((,table-name . "text") (,schema . "text"))
                    `((,table-name . "text"))))
            (res (pg-exec-prepared con sql args)))
       (cl-first (pg-result res :tuple 0))))
    (_
     (let* ((schema (when (pg-qualified-name-p table)
                      (pg-qualified-name-schema table)))
            (table-name (if (pg-qualified-name-p table)
                            (pg-qualified-name-name table)
                          table))
            (schema-sql (if schema " AND schemaname=$2" ""))
            (sql (concat "SELECT tableowner FROM pg_catalog.pg_tables WHERE tablename=$1" schema-sql))
            (args (if schema
                      `((,table-name . "text") (,schema . "text"))
                    `((,table-name . "text"))))
            (res (pg-exec-prepared con sql args)))
       (cl-first (pg-result res :tuple 0))))))

(defun pg--table-classoid (con table)
  "Return the OID of the class for PostgreSQL TABLE.
Uses database connection CON."
  (let* ((table-name (if (pg-qualified-name-p table) (pg-qualified-name-name table) table))
         (schema-name (when (pg-qualified-name-p table) (pg-qualified-name-schema table)))
         (relnamespace (when schema-name
                         (let ((res (pg-exec-prepared
                                     con
                                     "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=$1"
                                     `((,schema-name . "text")))))
                           (cl-first (pg-result res :tuple 0)))))
         (sql/noschema "SELECT oid FROM pg_catalog.pg_class WHERE relkind='r' AND relname=$1")
         (sql/wschema "SELECT oid FROM pg_catalog.pg_class WHERE relkind='r' AND relname=$1 AND relnamespace=$2")
         (res (if schema-name
                  (pg-exec-prepared con sql/wschema `((,table-name . "text") (,relnamespace . "int4")))
                (pg-exec-prepared con sql/noschema `((,table-name . "text")))))
         (row (pg-result res :tuple 0)))
    (when (null row)
      (let ((msg (format "Can't find classoid for table %s" table)))
        (signal 'pg-user-error (list msg))))
    (cl-first row)))

;; As per https://www.postgresql.org/docs/current/sql-comment.html. But many PostgreSQL variants do
;; not implement this functionality, or annoyingly use different SQL syntax for it.
(defun pg-table-comment (con table)
  "Return the comment on TABLE in a PostgreSQL database.
TABLE can be a string or a schema-qualified name. Uses database connection CON."
  (pcase (pgcon-server-variant con)
    ('cratedb nil)
    ('questdb nil)
    ('spanner nil)
    ('ydb nil)
    ;; As of 2025-08, CedarDB returns "Setting comments in not implemented yet" (sic).
    ('cedardb nil)
    ;; Our query below using PostgreSQL system tables triggers an internal exception in CockroachDB,
    ;; so we use their non-standard "SHOW TABLES" query. The SHOW TABLES command does not accept a
    ;; WHERE clause.
    ('cockroachdb
     (let* ((table-name (if (pg-qualified-name-p table) (pg-qualified-name-name table) table))
            (schema-name (when (pg-qualified-name-p table) (pg-qualified-name-schema table)))
            (res (pg-exec con "SHOW TABLES WITH COMMENT"))
            (tuples (pg-result res :tuples))
            (column-names (mapcar #'cl-first (pg-result res :attributes)))
            (table-name-pos (or (cl-position "table_name" column-names :test #'string=)
                                (error "Expecting table_name in SHOW TABLES output")))
            (table-schema-pos (or (cl-position "schema_name" column-names :test #'string=)
                                  (error "Expecting schema_name in SHOW TABLES output")))
            (comment-pos (or (cl-position "comment" column-names :test #'string=)
                             (error "Expecting comment in SHOW TABLES output"))))
       (cl-loop
        for tuple in tuples
        when (and (string= table-name (nth table-name-pos tuple))
                  (or (not schema-name)
                      (string= schema-name (nth table-schema-pos tuple))))
        return (nth comment-pos tuple))))
    ('risingwave
     ;; RisingWave implements the obj_description() function, but annoyingly returns empty values
     ;; even when comments are defined. Comment data is available in the rw_description table.
     (let* ((classoid (pg--table-classoid con table))
            (sql "SELECT description FROM rw_catalog.rw_description WHERE objoid=$1 AND objsubid IS NULL")
            (res (pg-exec-prepared con sql `((,classoid . "int4"))))
            (tuple (pg-result res :tuple 0))
            (maybe-comment (cl-first tuple)))
       (if (equal maybe-comment pg-null-marker) nil maybe-comment)))
    ;; TODO: possibly some other PostgreSQL variants use the syntax "COMMENT ON TABLE tname" to
    ;; query the comment.
    (_ (let* ((t-id (pg-escape-identifier table))
              ;; TODO: use an SQL query that avoids escaping the table identifier.
              (sql "SELECT obj_description($1::regclass::oid, 'pg_class')")
              (res (pg-exec-prepared con sql `((,t-id . "text"))))
              (tuple (pg-result res :tuple 0))
              (maybe-comment (cl-first tuple)))
         (if (equal maybe-comment pg-null-marker) nil maybe-comment)))))

;; Support for (setf (pg-table-comment con table) "comment")
(gv-define-setter pg-table-comment (comment con table)
  `(pcase (pgcon-server-variant ,con)
     ('cratedb nil)
     ('questdb nil)
     ('spanner nil)
     ('ydb nil)
     ('cedardb nil)
     ;; TheNile raises an error "command tag COMMENT unhandled"
     ('thenile nil)
     (_
      (let* ((cmt (if ,comment
                      (pcase (pgcon-server-variant ,con)
                        ;; RisingWave does not support the escaped literal format E'foo' that is
                        ;; used by pg-escape-identifier.
                        ('risingwave (concat "'" ,comment "'"))
                        (_ (pg-escape-literal ,comment)))
                    "NULL"))
             (sql (format "COMMENT ON TABLE %s IS %s"
                          (pg-escape-identifier ,table)
                          cmt)))
        ;; We can't use a prepared statement in this situation.
        (pg-exec ,con sql)
        ,comment))))

(defun pg-table-acl (con table)
  "Return the access control list for TABLE. Uses database connection CON.
Return nil if no ACL is defined, or if the pg_get_acl query function is not defined."
  ;; the pg_get_acl() function was introduced in PostgreSQL 18
  (when (>= (pgcon-server-version-major con) 18)
    (let* ((default-schema (if (eq (pgcon-server-variant con) 'cratedb)
                               "postgres"
                             "public"))
           (schema (if (pg-qualified-name-p table)
                       (pg-qualified-name-schema table)
                     default-schema))
           (tname (if (pg-qualified-name-p table)
                      (pg-qualified-name-name table)
                    table))
           (sql "SELECT pg_get_acl('pg_class'::regclass, c.oid, 0) FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2")
           (res (pg-exec-prepared con sql `((,schema . "text") (,tname . "text")))))
      (cl-first (pg-result res :tuple 0)))))

(defun pg-function-p (con name)
  "Return non-null when a function with NAME is defined in PostgreSQL.
Uses database connection CON."
  (pcase (pgcon-server-variant con)
    ;; The pg_proc table exists, but is empty.
    ('risingwave
     (signal 'pg-user-error (list "pg-function-p not implemented for Risingwave")))
    ;; QuestDB does not implement the pg_proc table.
    ('questdb
     (let* ((sql "SELECT name FROM functions() WHERE name=$1")
            (res (pg-exec-prepared con sql `((,name . "text"))))
            (rows (pg-result res :tuples)))
       (not (null rows))))
       ;; (cl-position name rows :key #'cl-first :test #'string=)))
    ('vertica
     ;; Vertica provides the v_catalog.user_functions and v_catalog.user_procedures tables that list
     ;; all user-defined functions and procedures, but current versions do not have any information
     ;; on builtin functions or procedures.
     (signal 'pg-user-error (list "pg-function-p not implemented for Vertica")))
    (_
     (let* ((sql "SELECT * FROM pg_catalog.pg_proc WHERE proname = $1")
            (res (pg-exec-prepared con sql `((,name . "text")))))
       (pg-result res :tuples)))))



;; DBMS metainformation ================================================
;;
;; Metainformation such as the list of databases present in the database management system, list of
;; tables, attributes per table. This information is not available directly, but can be obtained by
;; querying the system tables.
;;
;; Based on the queries issued by psql in response to user commands `\d' and `\d tablename'; see
;; file /usr/local/src/pgsql/src/bin/psql/psql.c
;; =====================================================================
(defun pg-databases (con)
  "List of the databases in the PostgreSQL server we are connected to via CON."
  (pcase (pgcon-server-variant con)
    ('vertica
     (let* ((sql "SELECT database_name FROM v_catalog.databases")
            (res (pg-exec con sql)))
       (apply #'append (pg-result res :tuples))))
    ('arcadedb
     (let ((res (pg-exec con "SELECT FROM schema:database")))
       (apply #'append (pg-result res :tuples))))
    (_
     (let ((res (pg-exec con "SELECT datname FROM pg_catalog.pg_database")))
       (apply #'append (pg-result res :tuples))))))

(defun pg-current-schema (con)
  "Return the current schema in the PostgreSQL server we are connected to via CON."
  (pcase (pgcon-server-variant con)
    ('clickhouse
     (let* ((res (pg-exec con "SELECT currentDatabase()"))
            (row (pg-result res :tuple 0)))
       (cl-first row)))
    ('arcadedb nil)
    ('datafusion nil)
    ('stoolap nil)
    ('immudb nil)
    (_
     (let* ((res (pg-exec con "SELECT current_schema()"))
            (tuple (pg-result res :tuple 0))
            (maybe-schema (cl-first tuple)))
       (if (equal maybe-schema pg-null-marker) nil maybe-schema)))))

;; Possible alternative query:
;;   SELECT nspname FROM pg_namespace
(defun pg-schemas (con)
  "List of the schemas in the PostgreSQL database we are connected to via CON."
  (pcase (pgcon-server-variant con)
    ;; QuestDB doesn't really support schemas.
    ('questdb (list "sys" "public"))
    ('arcadedb nil)
    ('datafusion nil)
    ('stoolap nil)
    ('immudb nil)
    ((or 'risingwave 'octodb 'pgsqlite)
     (let ((res (pg-exec con "SELECT DISTINCT table_schema FROM information_schema.tables")))
       (apply #'append (pg-result res :tuples))))
    ('vertica
     (let ((res (pg-exec con "SELECT DISTINCT schema_name FROM v_catalog.schemata")))
       (apply #'append (pg-result res :tuples))))
    (_
     (let ((res (pg-exec con "SELECT schema_name FROM information_schema.schemata")))
       (apply #'append (pg-result res :tuples))))))

(defun pg--tables-information-schema (con)
  "List of the tables present in the database we are connected to via CON.
Queries the information schema."
  (let* ((default-schema (if (eq (pgcon-server-variant con) 'cratedb)
                             "postgres"
                           "public"))
         (res (pg-exec con "SELECT DISTINCT table_schema,table_name FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_type='BASE TABLE'")))
    (cl-loop
     for tuple in (pg-result res :tuples)
     collect (let ((schema (cl-first tuple))
                   (name (cl-second tuple)))
               (if (string= schema default-schema)
                   name
                 (make-pg-qualified-name :schema schema :name name))))))

;; This method is better supported on very old PostgreSQL versions, or some semi-compatible
;; PostgreSQL databases that don't fully implement the information schema.
(defun pg--tables-legacy (con)
  "List of the tables present in the database we are connected to via CON.
Queries legacy internal PostgreSQL tables."
  (let ((res (pg-exec con "SELECT relname FROM pg_catalog.pg_class c WHERE "
                      "c.relkind = 'r' AND "
                      "c.relname !~ '^pg_' AND "
                      "c.relname !~ '^sql_' ORDER BY relname")))
    (apply #'append (pg-result res :tuples))))

;; Exclude Materialize-internal tables (which are in Materialize-specific schemata) from the list of
;; tables returned by pg-tables.
(defun pg--tables-materialize (con)
  (let ((res (pg-exec con "SELECT table_schema,table_name FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'mz_catalog', 'mz_internal', 'mz_introspection')
                AND table_type='BASE TABLE'")))
    (cl-loop
     for tuple in (pg-result res :tuples)
     collect (let ((schema (cl-first tuple))
                   (name (cl-second tuple)))
               (if (string= schema "public")
                   name
                 (make-pg-qualified-name :schema schema :name name))))))

;; Exclude TimescaleDB-internal tables (which are in TimescaleDB-specific schemata) from the list of
;; tables returned by pg-tables.
(defun pg--tables-timescaledb (con)
  (cl-labels ((timescale-name-p (tbl)
                (when (pg-qualified-name-p tbl)
                  (let ((s (pg-qualified-name-schema tbl)))
                    (cl-find s '("_timescaledb_cache"
                                 "_timescaledb_catalog"
                                 "_timescaledb_internal"
                                 "_timescaledb_config")
                             :test #'string=)))))
    (cl-delete-if #'timescale-name-p (pg--tables-information-schema con))))

;; Exclude CrateDB-internal tables (which are in the "sys" schemata) from the list of
;; tables returned by pg-tables.
(defun pg--tables-cratedb (con)
  (cl-labels ((cratedb-name-p (tbl)
                (when (pg-qualified-name-p tbl)
                  (string= "sys" (pg-qualified-name-schema tbl)))))
    (cl-delete-if #'cratedb-name-p (pg--tables-information-schema con))))

;; Exclude Clickhouse-internal tables from the list of tables returned by pg-tables.
;;
;; We could also use the query
;;   SELECT name FROM system.tables WHERE database == currentDatabase()
(defun pg--tables-clickhouse (con)
  (cl-labels ((clickhouse-name-p (tbl)
                (when (pg-qualified-name-p tbl)
                  (string= "system" (pg-qualified-name-schema tbl)))))
    (cl-delete-if #'clickhouse-name-p (pg--tables-information-schema con))))

(defun pg--tables-ydb (con)
  (let* ((sql "SELECT schemaname,tablename FROM pg_catalog.pg_tables WHERE hasindexes=true")
         (res (pg-exec con sql))
         (rows (pg-result res :tuples)))
    (cl-loop
     for row in rows
     collect (make-pg-qualified-name :schema (cl-first row) :name (cl-second row)))))

;; https://docs.vertica.com/24.2.x/en/sql-reference/system-tables/v-catalog-schema/
(defun pg--tables-vertica (con)
  (let* ((sql "SELECT table_schema,table_name FROM v_catalog.tables")
         (res (pg-exec con sql))
         (rows (pg-result res :tuples)))
    (cl-loop
     for row in rows
     collect (make-pg-qualified-name :schema (cl-first row) :name (cl-second row)))))

(defun pg--tables-arcadedb (con)
  (let ((res (pg-exec con "SELECT FROM schema:types")))
    (apply #'append (pg-result res :tuples))))

(defun pg--tables-stoolap (con)
  (let ((res (pg-exec con "SHOW TABLES")))
    (apply #'append (pg-result res :tuples))))


(defun pg-tables (con)
  "List of the tables present in the database we are connected to via CON.
Only tables to which the current user has access are listed."
    (cond ((eq (pgcon-server-variant con) 'ydb)
           (pg--tables-ydb con))
          ((eq (pgcon-server-variant con) 'timescaledb)
           (pg--tables-timescaledb con))
          ((eq (pgcon-server-variant con) 'cratedb)
           (pg--tables-cratedb con))
          ((eq (pgcon-server-variant con) 'materialize)
           (pg--tables-materialize con))
          ((eq (pgcon-server-variant con) 'clickhouse)
           (pg--tables-clickhouse con))
          ((eq (pgcon-server-variant con) 'vertica)
           (pg--tables-vertica con))
          ((eq (pgcon-server-variant con) 'arcadedb)
           (pg--tables-arcadedb con))
          ((eq (pgcon-server-variant con) 'stoolap)
           (pg--tables-stoolap con))
          ((eq (pgcon-server-variant con) 'octodb)
           (pg--tables-legacy con))
          ((> (pgcon-server-version-major con) 11)
           (pg--tables-information-schema con))
          (t
           (pg--tables-legacy con))))

(defun pg--columns-information-schema (con table)
  (let* ((default-schema (if (eq (pgcon-server-variant con) 'cratedb)
                             "postgres"
                           "public"))
         (schema (if (pg-qualified-name-p table)
                     (pg-qualified-name-schema table)
                   default-schema))
         (tname (if (pg-qualified-name-p table)
                    (pg-qualified-name-name table)
                  table))
         (sql "SELECT column_name FROM information_schema.columns
               WHERE table_schema=$1 AND table_name = $2")
         (res (pg-exec-prepared con sql `((,schema . "text") (,tname . "text")))))
    (apply #'append (pg-result res :tuples))))

(defun pg--columns-legacy (con table)
  (let* ((sql (format "SELECT * FROM %s WHERE 0 = 1" table))
         (res (pg-exec con sql)))
    (mapcar #'car (pg-result res :attributes))))

(defun pg-columns (con table)
  "List of the columns present in TABLE over PostgreSQL connection CON."
  (cond ((member (pgcon-server-variant con) '(ydb vertica))
         (pg--columns-legacy con table))
        ((> (pgcon-server-version-major con) 7)
         (pg--columns-information-schema con table))
        (t
         (pg--columns-legacy con table))))

(defun pg-column-default/full (con table column)
  "Return the default value for COLUMN in PostgreSQL TABLE.
Using connection to PostgreSQL CON."
  (let* ((schema (if (pg-qualified-name-p table)
                     (pg-qualified-name-schema table)
                   "public"))
         (tname (if (pg-qualified-name-p table)
                    (pg-qualified-name-name table)
                  table))
         (sql "SELECT column_default FROM information_schema.columns
               WHERE (table_schema, table_name, column_name) = ($1, $2, $3)")
         (argument-types (list "text" "text" "text"))
         (params `((,schema . "text") (,tname . "text") (,column . "text")))
         (ps-name (pg-ensure-prepared-statement con "QRY-column-default" sql argument-types))
         (res (pg-fetch-prepared con ps-name params))
         (tuple (pg-result res :tuple 0))
         (maybe-comment (cl-first tuple)))
    (if (equal maybe-comment pg-null-marker) nil maybe-comment)))

(defun pg-column-default (con table column)
  "Return the default value for COLUMN in PostgreSQL TABLE.
Using connection to PostgreSQL CON."
  (pcase (pgcon-server-variant con)
    ('cratedb nil)
    ('questdb nil)
    ('ydb nil)
    ;; TODO: Materialize is incorrectly returning "DEFAULT NULL" for the query used in
    ;; pg-column-default/full; we could try to add a workaround.
    (_ (pg-column-default/full con table column))))

(defun pg-column-comment (con table column)
  "Return the comment on COLUMN in TABLE in a PostgreSQL database.
TABLE can be a string or a schema-qualified name. Uses database connection CON."
  (pcase (pgcon-server-variant con)
    ('cratedb nil)
    ('spanner nil)
    ('questdb nil)
    ('ydb nil)
    ;; RisingWave implements the col_description() function, but annoyingly returns empty values
    ;; even when comments are defined. Comment data is available in the rw_description table.
    ('risingwave
     (let* ((classoid (pg--table-classoid con table))
            (t-id (pg-escape-identifier table))
            (res (pg-exec con (format "SELECT * FROM %s LIMIT 0" t-id)))
            (column-number (or (cl-position column (pg-result res :attributes)
                                            :key #'cl-first
                                            :test #'string=)
                               (signal 'pg-user-error (list (format "Column %s not found in table %s"
                                                                    column table)))))
            (sql "SELECT description FROM rw_catalog.rw_description WHERE objoid=$1 AND objsubid=$2")
            (res (pg-exec-prepared con sql `((,classoid . "int4") (,(1+ column-number) . "int4"))))
            (row (pg-result res :tuple 0)))
       (cl-first row)))
    (_ (let* ((t-id (pg-escape-identifier table))
              (res (pg-exec con (format "SELECT * FROM %s LIMIT 0" t-id)))
              (column-number (or (cl-position column (pg-result res :attributes)
                                              :key #'cl-first
                                              :test #'string=)
                                 (signal 'pg-user-error (list (format "Column %s not found in table %s"
                                                                      column table)))))
              (sql "SELECT pg_catalog.col_description($1::regclass::oid, $2)")
              (res (pg-exec-prepared con sql `((,t-id . "text") (,(1+ column-number) . "int4"))))
              (tuple (pg-result res :tuple 0))
              (maybe-comment (cl-first tuple)))
         (if (equal maybe-comment pg-null-marker) nil maybe-comment)))))

(gv-define-setter pg-column-comment (comment con table column)
  `(pcase (pgcon-server-variant ,con)
     ('cratedb nil)
     ('questdb nil)
     ('spanner nil)
     (_
      (let* ((cmt (if ,comment
                      (pcase (pgcon-server-variant ,con)
                        ;; RisingWave does not support the escaped literal format E'foo' that is
                        ;; used by pg-escape-identifier.
                        ('risingwave (concat "'" ,comment "'"))
                        (_ (pg-escape-literal ,comment)))
                    "NULL"))
             (sql (format "COMMENT ON COLUMN %s.%s IS %s"
                          (pg-escape-identifier ,table)
                          (pg-escape-identifier ,column)
                          cmt)))
        ;; We can't use a prepared statement in this situation.
        (pg-exec ,con sql)
        ,comment))))

;; This returns non-nil for columns for which you can insert a row without specifying a value for
;; the column. That includes columns:
;;
;;    - with a specified DEFAULT (including SERIAL columns)
;;    - specified as "BIGINT GENERATED ALWAYS AS IDENTITY"
;;    - specified as "GENERATED ALWAYS AS expr STORED" (calculated from other columns)
(defun pg-column-autogenerated-p/full (con table column)
  "Return non-nil if COLUMN has an SQL default value or is autogenerated.
COLUMN is in TABLE. Uses connection to PostgreSQL CON."
  (let* ((schema (if (pg-qualified-name-p table)
                     (pg-qualified-name-schema table)
                   "public"))
         (tname (if (pg-qualified-name-p table)
                    (pg-qualified-name-name table)
                  table))
         (sql "SELECT true FROM information_schema.columns
               WHERE (table_schema, table_name, column_name) = ($1, $2, $3)
               AND (column_default IS NOT NULL OR is_generated='ALWAYS' OR is_identity='YES')")
         (argument-types (list "text" "text" "text"))
         (params `((,schema . "text") (,tname . "text") (,column . "text")))
         (ps-name (pg-ensure-prepared-statement con "QRY-column-autogenerated" sql argument-types))
         (res (pg-fetch-prepared con ps-name params)))
    (caar (pg-result res :tuples))))

;; CrateDB does not support the is_generated and is_identity columns in the
;; information_schema.columns table.
;;
;; Materialize is buggy on this query: it has column_default='NULL' as a text value, rather than
(defun pg-column-autogenerated-p/simple (con table column)
  "Return non-nil if COLUMN has an SQL default value or is autogenerated.
COLUMN is in TABLE. Uses connection to PostgreSQL CON."
  (let* ((schema (if (pg-qualified-name-p table)
                     (pg-qualified-name-schema table)
                   "public"))
         (tname (if (pg-qualified-name-p table)
                    (pg-qualified-name-name table)
                  table))
         ;; CrateDB does not support tuple comparison WHERE (col1, col2) = (1, 2)
         (sql "SELECT true FROM information_schema.columns
               WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
               AND column_default IS NOT NULL")
         (argument-types (list "text" "text" "text"))
         (params `((,schema . "text") (,tname . "text") (,column . "text")))
         (ps-name (pg-ensure-prepared-statement con "QRY-column-autogenerated" sql argument-types))
         (res (pg-fetch-prepared con ps-name params)))
    (caar (pg-result res :tuples))))

(defun pg-column-autogenerated-p (con table column)
  "Return non-nil if COLUMN has an SQL default value or is autogenerated.
COLUMN is in TABLE. Uses connection to PostgreSQL CON."
  (pcase (pgcon-server-variant con)
    ((or 'cratedb 'materialize 'questdb)
     (pg-column-autogenerated-p/simple con table column))
    (_ (pg-column-autogenerated-p/full con table column))))


(defun pg-backend-version (con)
  "Return version and operating environment of PostgreSQL backend.
Concerns the backend that we are connected to over connection CON.
PostgreSQL returns the version as a string. CrateDB returns it as an integer."
  (let ((res (pg-exec con "SELECT version()")))
    (cl-first (pg-result res :tuple 0))))


;; support routines ============================================================

;; Called to handle a RowDescription message
(defun pg--read-attributes (con)
  "Read RowDescription attributes from PostgreSQL connection CON."
  (let* ((_msglen (pg--read-net-int con 4))
         (attribute-count (pg--read-net-int con 2))
         (attributes (list))
         (ce (pgcon-client-encoding con)))
    (cl-do ((i attribute-count (- i 1)))
        ((zerop i) (nreverse attributes))
      (let ((type-name  (pg--read-string con))
            (_table-oid (pg--read-net-int con 4))
            (_col       (pg--read-net-int con 2))
            (type-oid   (pg--read-net-int con 4))
            (type-len   (pg--read-net-int con 2))
            (_type-mod  (pg--read-net-int con 4))
            (_format-code (pg--read-net-int con 2)))
        (push (list (pg-text-parser type-name ce) type-oid type-len) attributes)))))

;; Read data following a DataRow message
(defun pg--read-tuple (con attributes)
  "Read a tuple from a DataRow message on PostgreSQL connection CON.
The RowDescription data is provided in ATTRIBUTES."
  (let* ((num-attributes (length attributes))
         (col-count (pg--read-net-int con 2))
         (tuples (list)))
    (unless (eql col-count num-attributes)
      (signal 'pg-protocol-error '("Unexpected value for attribute count sent by backend")))
    (cl-do ((i 0 (+ i 1))
            (type-ids (mapcar #'cl-second attributes) (cdr type-ids)))
        ((= i num-attributes) (nreverse tuples))
      (let ((col-octets (pg--read-net-int con 4)))
        (cl-case col-octets
          (4294967295
           ;; this is "-1" (pg--read-net-int doesn't handle integer overflow), which indicates a
           ;; NULL column
           (push pg-null-marker tuples))
          (0
           (push "" tuples))
          (t
           (let* ((col-value (pg--read-chars con col-octets))
                  (parsed (pg-parse con col-value (car type-ids))))
             (push parsed tuples))))))))

(defun pg--read-char (con)
  "Read a single character from PostgreSQL connection CON."
  (declare (speed 3))
  (let ((process (pgcon-process con)))
    ;; (accept-process-output process 0.1)
    (with-current-buffer (process-buffer process)
      (when (null (char-after pgcon--position))
        (dotimes (_i (pgcon-timeout con))
          (when (null (char-after pgcon--position))
            (when (eq system-type 'windows-nt)
              (sit-for 0.1))
            (accept-process-output process 1.0))))
      (when (null (char-after pgcon--position))
        (let ((msg (format "Timeout in pg--read-char reading from %s" con)))
          (signal 'pg-timeout (list msg))))
      (prog1 (char-after pgcon--position)
        (setq-local pgcon--position (1+ pgcon--position))))))

(defun pg--unread-char (con)
  (let ((process (pgcon-process con)))
    (with-current-buffer (process-buffer process)
      (setq-local pgcon--position (1- pgcon--position)))))

;; FIXME should be more careful here; the integer could overflow.
(defun pg--read-net-int (con bytes)
  (declare (speed 3))
  (cl-do ((i bytes (- i 1))
          (accum 0))
      ((zerop i) accum)
    (setq accum (+ (* 256 accum) (pg--read-char con)))))

(defun pg--read-int (con bytes)
  (declare (speed 3))
  (cl-do ((i bytes (- i 1))
          (multiplier 1 (* multiplier 256))
          (accum 0))
      ((zerop i) accum)
    (cl-incf accum (* multiplier (pg--read-char con)))))

(defun pg--read-chars-old (con howmany)
  (cl-do ((i 0 (+ i 1))
          (chars (make-string howmany ?.)))
      ((= i howmany) chars)
    (aset chars i (pg--read-char con))))

(defun pg--read-chars (con count)
  (declare (speed 3))
  (let ((process (pgcon-process con)))
    (with-current-buffer (process-buffer process)
      (let* ((start pgcon--position)
             (end (+ start count)))
        ;; (accept-process-output process 0.1)
        (when (> end (point-max))
          (dotimes (_i (pgcon-timeout con))
            (when (> end (point-max))
              ;; (sleep-for 0.1)
              (accept-process-output process 1.0))))
        (when (> end (point-max))
          (let ((msg (format "Timeout in pg--read-chars reading from %s" con)))
            (signal 'pg-timeout (list msg))))
        (prog1 (buffer-substring-no-properties start end)
          (setq-local pgcon--position end))))))

(cl-defun pg--read-string (con &optional (max-bytes 1048576))
  "Read a null-terminated string from PostgreSQL connection CON.
If MAX-BYTES is specified, it designates the maximal number of octets
that will be read."
  (declare (speed 3))
  (cl-loop for i below max-bytes
           for ch = (pg--read-char con)
           until (eql ch ?\0)
           concat (byte-to-string ch)))

(cl-defstruct pgerror
  severity sqlstate message detail hint table column dtype file line routine where constraint)

(defun pg-read-error-response (con)
  (let* ((response-len (pg--read-net-int con 4))
         (msglen (- response-len 4))
         (msg (pg--read-chars con msglen))
         (msgpos 0)
         (err (make-pgerror))
         (ce (pgcon-client-encoding con)))
    (cl-loop while (< msgpos (1- msglen))
             for field = (aref msg msgpos)
             for val = (let* ((start (cl-incf msgpos))
                              (end (cl-position #x0 msg :start start :end msglen)))
                         (prog1
                             (substring msg start end)
                           (setf msgpos (1+ end))))
             ;; these field types: https://www.postgresql.org/docs/current/protocol-error-fields.html
             do (cl-case field
                  (?S
                   (setf (pgerror-severity err) val))
                  ;; This is the unlocalized severity name (only sent for PostgreSQL > 9.6). It's
                  ;; probably more useful to the user so we keep that.
                  (?V
                   (setf (pgerror-severity err) val))
                  (?C
                   (setf (pgerror-sqlstate err) val))
                  (?M
                   (setf (pgerror-message err)
                         (decode-coding-string val ce)))
                  (?D
                   (setf (pgerror-detail err)
                         (decode-coding-string val ce)))
                  (?H
                   (setf (pgerror-hint err)
                         (decode-coding-string val ce)))
                  (?F
                   (setf (pgerror-file err)
                         (decode-coding-string val ce)))
                  (?L
                   (setf (pgerror-line err)
                         (decode-coding-string val ce)))
                  (?R
                   (setf (pgerror-routine err)
                         (decode-coding-string val ce)))
                  (?W
                   (setf (pgerror-where err)
                         (decode-coding-string val ce)))
                  (?t
                   (setf (pgerror-table err) val))
                  (?c
                   (setf (pgerror-column err) val))
                  (?d
                   (setf (pgerror-dtype err) val))
                  (?n
                   (setf (pgerror-constraint err) val))))
    err))

(defun pg-log-notice (notice)
  "Log a NOTICE to the *Messages* buffer."
  (let ((extra (list)))
    (when (pgerror-detail notice)
      (push ", " extra)
      (push (pgerror-detail notice) extra))
    (when (pgerror-hint notice)
      (push ", " extra)
      (push (format "hint: %s" (pgerror-hint notice)) extra))
    (when (pgerror-table notice)
      (push ", " extra)
      (push (format "table: %s" (pgerror-table notice)) extra))
    (when (pgerror-column notice)
      (push ", " extra)
      (push (format "column: %s" (pgerror-column notice)) extra))
    (setf extra (nreverse extra))
    (pop extra)
    (setf extra (butlast extra))
    (when extra
      (setf extra (append (list " (") extra (list ")"))))
    (message "%sPostgreSQL %s %s %s%s"
             (if noninteractive "\033[38;5;248m" "")
             (pgerror-severity notice)
             (pgerror-message notice)
             (apply #'concat extra)
             (if noninteractive "\033[0m" ""))))

;; The next two functions are replacements for pg-flush and pg--buffered-send if we wish to use an
;; "immediate transmission" mode for our communication with the backend, rather than accumulating
;; output in the output buffer and sending it batched on a call to pg-flush. Immediate mode (which
;; was used prior to version 0.58 of this library) leads to greater packet fragmentation at the
;; network level, and is less efficient.
(defun pg-flush/immediate (con)
  (accept-process-output (pgcon-process con) 0.1))

(defun pg--buffered-send/immediate (con octets)
  (process-send-string (pgcon-process con) octets))

(defun pg-flush (con)
  (with-current-buffer (pgcon-output-buffer con)
    (process-send-string (pgcon-process con)
                         (buffer-substring-no-properties pgcon--position (point-max)))
    (setq pgcon--position (point-max)))
  (accept-process-output))

(defun pg--buffered-send (con octets)
  (with-current-buffer (pgcon-output-buffer con)
    (insert octets)))

;; higher order bits first / little endian
(defun pg--send-uint (con num bytes)
  (declare (speed 3))
  (let ((str (make-string bytes 0))
        (i (- bytes 1)))
    (while (>= i 0)
      (aset str i (% num 256))
      (setq num (floor num 256))
      (cl-decf i))
    (pg--buffered-send con str)))

;; big endian
(defun pg--send-net-uint (con num bytes)
  (declare (speed 3))
  (let ((str (make-string bytes 0)))
    (dotimes (i bytes)
      (aset str i (% num 256))
      (setq num (floor num 256)))
    (pg--buffered-send con str)))

(defun pg--send-char (con char)
  (pg--buffered-send con (char-to-string char)))

(defun pg--send-string (con string)
  (pg--buffered-send con string)
  ;; the null-terminator octet
  (pg--buffered-send con (unibyte-string 0)))

(defun pg--send-octets (con octets)
  (pg--buffered-send con octets))

(defun pg--send (con str &optional bytes)
  (declare (speed 3))
  (let ((padding (if (and (numberp bytes) (> bytes (length str)))
                     (make-string (- bytes (length str)) 0)
                   (make-string 0 0 nil))))
    (pg--buffered-send con (concat str padding))))


;; Mostly for debugging use. Doesn't kill lo buffers.
(defun pg-kill-all-buffers ()
  "Kill all buffers used for network connections with PostgreSQL."
  (interactive)
  (cl-loop for buffer in (buffer-list)
           for name = (buffer-name buffer)
           when (and (> (length name) 12)
                     (string= " *PostgreSQL*" (substring (buffer-name buffer) 0 13)))
           do (let ((p (get-buffer-process buffer)))
                (when p
                  (kill-process p)))
           (kill-buffer buffer)))

(provide 'pg)

;; Local Variables:
;; indent-tabs-mode: nil
;; End:
;;; pg.el ends here
