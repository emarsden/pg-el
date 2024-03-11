;;; pg.el --- Emacs Lisp socket-level interface to the PostgreSQL RDBMS  -*- lexical-binding: t -*-

;; Copyright: (C) 1999-2002, 2022-2024  Eric Marsden

;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; Version: 0.29
;; Keywords: data comm database postgresql
;; URL: https://github.com/emarsden/pg-el
;; Package-Requires: ((emacs "28.1"))
;; SPDX-License-Identifier: GPL-2.0-or-later
;;
;; This file is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published
;; by the Free Software Foundation, either version 2 of the License,
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
;; This module lets you access the PostgreSQL object-relational DBMS from Emacs, using its
;; socket-level frontend/backend protocol. The module is capable of automatic type coercions from a
;; range of SQL types to the equivalent Emacs Lisp type. This is a low level API, and won't be
;; useful to end users.
;;
;; Supported features:
;;
;;  - SCRAM-SHA-256 authentication (the default method since PostgreSQL version 14) and MD5
;;  - authentication.
;;
;;  - Encrypted (TLS) connections between Emacs and the PostgreSQL backend.
;;
;;  - Support for the SQL COPY protocol to copy preformatted data to PostgreSQL from an Emacs
;;  - buffer.
;;
;;  - Asynchronous handling of LISTEN/NOTIFY notification messages from PostgreSQL, allowing the
;;    implementation of publish-subscribe type architectures (PostgreSQL as an "event broker" or
;;    "message bus" and Emacs as event publisher and consumer.
;;
;;  - Support for PostgreSQL's extended query syntax, that allows for parameterized queries to
;;    protect from SQL injection issues.
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
;; * Provide support for client-side certificates to authenticate network
;;   connections over TLS.
;;
;; * Implement the SASLPREP algorithm for usernames and passwords that contain
;;   unprintable characters (used for SCRAM-SHA-256 authentication).
;;
;; * Add a mechanism for parsing user-defined types. The user should be able to define a parse
;;   function and a type-name; we query pg_type to get the type's OID and add the information to
;;   pg--parsers.


;;; Code:

(require 'cl-lib)
(require 'hex-util)
(require 'bindat)
(require 'url)

(defvar pg-application-name (or (getenv "PGAPPNAME") "pg.el")
  "The application_name sent to the PostgreSQL backend.
This information appears in queries to the `pg_stat_activity' table
and (depending on server configuration) in the connection log.")


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

(define-error 'pg-error "PostgreSQL error" 'error)
(define-error 'pg-protocol-error "PostgreSQL protocol error" 'pg-error)
(define-error 'pg-copy-failed "PostgreSQL COPY failed" 'pg-error)

;; Maps from type-name to a function that converts from text representation to wire-level binary
;; representation.
(defvar pg--serializers (make-hash-table :test #'equal))

;; Maps from type-name to a parsing function (from string to Emacs native type). This is built
;; dynamically at initialization of the connection with the database (once generated, the
;; information is shared between connections).
(defvar pg--parsers-name (make-hash-table :test #'equal))

;; Maps from oid (an integer) to a parsing function.
(defvar pg--parsers-oid (make-hash-table :test #'eql))

;; Maps from type-name to PostgreSQL oid.
(defvar pg--type-oid (make-hash-table :test #'equal))

;; Maps from oid to type-name.
(defvar pg--type-name (make-hash-table :test #'eql))


(cl-defstruct pgcon
  dbname
  process
  pid
  server-version-major
  secret
  (client-encoding 'utf-8)
  (timeout 10)
  connect-info)

;; Used to save the connection-specific position in our input buffer.
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


(cl-defstruct pgresult
  connection status attributes tuples portal (incomplete nil))

(defsubst pg-flush (con)
  (accept-process-output (pgcon-process con) 1))

;; this is ugly because lambda lists don't do destructuring
(defmacro with-pg-connection (con connect-args &rest body)
  "Execute BODY forms in a scope with connection CON created by CONNECT-ARGS.
The database connection is bound to the variable CON. If the
connection is unsuccessful, the forms are not evaluated.
Otherwise, the BODY forms are executed, and upon termination,
normal or otherwise, the database connection is closed."
  `(let ((,con (pg-connect ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg-disconnect ,con)))))
(put 'with-pg-connection 'lisp-indent-function 'defun)

(defmacro with-pg-connection-local (con connect-args &rest body)
  "Execute BODY forms in a scope with local Unix connection CON created by CONNECT-ARGS.
The database connection is bound to the variable CON. If the
connection is unsuccessful, the forms are not evaluated.
Otherwise, the BODY forms are executed, and upon termination,
normal or otherwise, the database connection is closed."
  `(let ((,con (pg-connect-local ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg-disconnect ,con)))))
(put 'with-pg-connection 'lisp-indent-function 'defun)


(defmacro with-pg-transaction (con &rest body)
  "Execute BODY forms in a BEGIN..END block with pre-established connection CON.
If a PostgreSQL error occurs during execution of the forms, execute
a ROLLBACK command.
Large-object manipulations _must_ occur within a transaction, since
the large object descriptors are only valid within the context of a
transaction."
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


(defconst pg--AUTH_REQ_OK       0)
(defconst pg--AUTH_REQ_KRB4     1)
(defconst pg--AUTH_REQ_KRB5     2)
(defconst pg--AUTH_REQ_PASSWORD 3)   ; AuthenticationCleartextPassword
(defconst pg--AUTH_REQ_CRYPT    4)

(defconst pg--STARTUP_MSG            7)
(defconst pg--STARTUP_KRB4_MSG      10)
(defconst pg--STARTUP_KRB5_MSG      11)
(defconst pg--STARTUP_PASSWORD_MSG  14)

(defconst pg--MAX_MESSAGE_LEN    8192)   ; libpq-fe.h

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
                          1)))
    (pg-send-uint con packet-octets 4)
    (pg-send-uint con 3 2)              ; Protocol major version = 3
    (pg-send-uint con 0 2)              ; Protocol minor version = 0
    (pg-send-string con "user")
    (pg-send-string con user)
    (pg-send-string con "database")
    (pg-send-string con dbname)
    (pg-send-string con "application_name")
    (pg-send-string con pg-application-name)
    ;; A zero byte is required as a terminator after the last name/value pair.
    (pg-send-uint con 0 1)
    (pg-flush con))
  (cl-loop
   for c = (pg-read-char con) do
   (cl-case c
     ;; an ErrorResponse message
     (?E
      (pg-handle-error-response con "after StartupMessage"))

     ;; NegotiateProtocolVersion
     (?v
      (let ((_msglen (pg-read-net-int con 4))
            (protocol-supported (pg-read-net-int con 4))
            (unrec-options (pg-read-net-int con 4))
            (unrec (list)))
        ;; read the list of protocol options not supported by the server
        (dotimes (_i unrec-options)
          (push (pg-read-string con 4096) unrec))
        (let ((msg (format "Server only supports protocol minor version <= %s" protocol-supported)))
          (signal 'pg-protocol-error (list msg)))))

     ;; BackendKeyData
     (?K
      (let ((_msglen (pg-read-net-int con 4)))
        (setf (pgcon-pid con) (pg-read-net-int con 4))
        (setf (pgcon-secret con) (pg-read-net-int con 4))))

     ;; NoticeResponse
     (?N
      ;; a Notice response has the same structure and fields as an ErrorResponse
      (let ((notice (pg-read-error-response con)))
        (dolist (handler pg-handle-notice-functions)
          (funcall handler notice))))

     ;; ReadyForQuery message
     (?Z
      (let ((_msglen (pg-read-net-int con 4))
            (status (pg-read-char con)))
        ;; status is 'I' or 'T' or 'E', Idle or InTransaction or Error
        (when (eql ?E status)
          (message "PostgreSQL ReadyForQuery message with error status"))
        (and (not pg-disable-type-coercion)
             (zerop (hash-table-count pg--parsers-oid))
             (pg-initialize-parsers con))
        (pg-exec con "SET datestyle = 'ISO'")
        (pg-enable-async-notification-handlers con)
        (pg-connection-set-busy con nil)
        (cl-return-from pg-do-startup con)))

     ;; an authentication request
     (?R
      (let ((_msglen (pg-read-net-int con 4))
            (areq (pg-read-net-int con 4)))
        (cond
         ;; AuthenticationOK message
         ((= areq pg--AUTH_REQ_OK)
          ;; Continue processing server messages and wait for the ReadyForQuery
          ;; message
          nil)

         ((= areq pg--AUTH_REQ_PASSWORD)
          ;; send a PasswordMessage
          (pg-send-char con ?p)
          (pg-send-uint con (+ 5 (length password)) 4)
          (pg-send-string con password)
          (pg-flush con))
         ;; AuthenticationSASL request
         ((= areq 10)
          (pg-do-sasl-authentication con user password))
         ((= areq 5)
          (pg-do-md5-authentication con user password))
         ((= areq pg--AUTH_REQ_CRYPT)
          (signal 'pg-protocol-error '("Crypt authentication not supported")))
         ((= areq pg--AUTH_REQ_KRB4)
          (signal 'pg-protocol-error '("Kerberos4 authentication not supported")))
         ((= areq pg--AUTH_REQ_KRB5)
          (signal 'pg-protocol-error '("Kerberos5 authentication not supported")))
         (t
          (let ((msg (format "Can't do that type of authentication: %s" areq)))
            (signal 'pg-protocol-error (list msg)))))))

     ;; ParameterStatus
     (?S
      (let* ((msglen (pg-read-net-int con 4))
             (msg (pg-read-chars con (- msglen 4)))
             (items (split-string msg (string 0))))
        ;; ParameterStatus items sent by the backend include application_name,
        ;; DateStyle, in_hot_standby, integer_datetimes
        (when (> (length (cl-first items)) 0)
          (when (string= "server_version" (cl-first items))
            (let ((major (cl-first (split-string (cl-second items) "."))))
              (setf (pgcon-server-version-major con) (string-to-number major))))
          (dolist (handler pg-parameter-change-functions)
            (funcall handler con (cl-first items) (cl-second items))))))

     (t
      (let ((msg (format "Problem connecting: expected an authentication response, got %s" c)))
        (signal 'pg-protocol-error (list msg)))))))


;; Avoid warning from the bytecode compiler
(declare-function gnutls-negotiate "gnutls.el")
(declare-function network-stream-certificate "network-stream.el")

(cl-defun pg-connect (dbname user
                             &optional
                             (password "")
                             (host "localhost")
                             (port 5432)
                             (tls nil))
  "Initiate a connection with the PostgreSQL backend over TCP.
Connect to the database DBNAME with the username USER, on PORT of
HOST, providing PASSWORD if necessary. Return a connection to the
database (as an opaque type). PORT defaults to 5432, HOST to
\"localhost\", and PASSWORD to an empty string. If TLS is non-NIL,
attempt to establish an encrypted connection to PostgreSQL."
  (let* ((buf (generate-new-buffer " *PostgreSQL*"))
         (process (open-network-stream "postgres" buf host port :coding nil
                                       :nowait t :nogreeting t))
         (con (make-pgcon :dbname dbname :process process)))
    (with-current-buffer buf
      (set-process-coding-system process 'binary 'binary)
      (set-buffer-multibyte nil)
      (setq-local pgcon--position 1
                  pgcon--busy t
                  pgcon--notification-handlers (list)))
    ;; Save connection info in the pgcon object, for possible later use by pg-cancel
    (setf (pgcon-connect-info con) (list :tcp host port dbname user password))
    ;; TLS connections to PostgreSQL are based on a custom STARTTLS-like connection upgrade
    ;; handshake. The frontend establishes an unencrypted network connection to the backend over the
    ;; standard port (normally 5432). It then sends an SSLRequest message, indicating the desire to
    ;; establish an encrypted connection. The backend responds with ?S to indicate that it is able
    ;; to support an encrypted connection. The frontend then runs TLS negociation to upgrade the
    ;; connection to an encrypted one.
    (when tls
      (require 'gnutls)
      (require 'network-stream)
      (unless (gnutls-available-p)
        (signal 'pg-error '("Connecting over TLS requires GnuTLS support in Emacs")))
      ;; send the SSLRequest message
      (pg-send-uint con 8 4)
      (pg-send-uint con 80877103 4)
      (pg-flush con)
      (unless (eql ?S (pg-read-char con))
        (signal 'pg-protocol-error (list "Couldn't establish TLS connection to PostgreSQL")))
      (let ((cert (network-stream-certificate host port nil)))
        (condition-case err
            ;; now do STARTTLS-like connection upgrade
            (gnutls-negotiate :process process
                              :hostname host
                              :keylist (and cert (list cert)))
          (gnutls-error
           (let ((msg (format "TLS error connecting to PostgreSQL: %s" (error-message-string err))))
             (signal 'pg-protocol-error (list msg)))))))
    ;; the remainder of the startup sequence is common to TCP and Unix socket connections
    (pg-do-startup con dbname user password)))

(cl-defun pg-connect-local (path dbname user &optional (password ""))
  "Initiate a connection with the PostgreSQL backend over local Unix socket PATH.
Connect to the database DBNAME with the username USER, providing
PASSWORD if necessary. Return a connection to the database (as an
opaque type). PASSWORD defaults to an empty string."
  (let* ((buf (generate-new-buffer " *PostgreSQL*"))
         (process (make-network-process :name "postgres" :buffer buf :family 'local :service path :coding nil))
         (connection (make-pgcon :dbname dbname :process process)))
    ;; Save connection info in the pgcon object, for possible later use by pg-cancel
    (setf (pgcon-connect-info connection) (list :local path dbname user password))
    (with-current-buffer buf
      (set-process-coding-system process 'binary 'binary)
      (set-buffer-multibyte nil)
      (setq-local pgcon--position 1
                  pgcon--busy t
                  pgcon--notification-handlers (list)))
    (pg-do-startup connection dbname user password)))

;; e.g. "host=localhost port=5432 dbname=mydb connect_timeout=10"
;; see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
(defun pg-connect/string (string)
  "Connect to PostgreSQL with parameters specified by connection string STRING.
A connection string is of the form `host=localhost port=5432 dbname=mydb'.
We do not support all the parameter keywords supported by libpq,
such as those which specify particular aspects of the TCP
connection to PostgreSQL (e.g. keepalives_interval). The
supported keywords are host, hostaddr, port, dbname, user,
password, sslmode (partial support) and application_name."
  (let* ((components (split-string string "[ \t]" t))
         (params (cl-loop
                  for c in components
                  for param-val = (split-string c "=" t "\s")
                  unless (eql 2 (length param-val))
                  do (error "Invalid connection string component %s" c)
                  collect (cons (cl-first param-val) (cl-second param-val))))
         (host (or (cdr (assoc "host" params))
                   (cdr (assoc "hostaddr" params))
                   "localhost"))
         (port (or (cdr (assoc "port" params)) 5432))
         (dbname (or (cdr (assoc "dbname" params))
                     (error "Database name not specified in connection string")))
         (user (or (cdr (assoc "user" params))
                   (error "User not specified in connection string")))
         (password (cdr (assoc "password" params)))
         (sslmode (cdr (assoc "sslmode" params)))
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
         (pg-application-name (or (cdr (assoc "application_name" params))
                                  pg-application-name)))
    (pg-connect dbname user password host port tls)))


(defun pg-parse-url (url)
  "Adaptation of function url-generic-parse-url that does not downcase
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
            (setq host (buffer-substring save-pos (point)))
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
		   (setq port (string-to-number port)))))

	  ;; Now point is on the / ? or # which terminates the
	  ;; authority, or at the end of the URI, or (if there is no
	  ;; authority) at the beginning of the absolute path.

          (setq save-pos (point))
          (if (string= "data" scheme)
	      ;; For the "data" URI scheme, all the rest is the FILE.
	      (setq file (buffer-substring save-pos (point-max)))
	    ;; For hysterical raisins, our data structure returns the
	    ;; path and query components together in one slot.
	    ;; 3.3. Path
	    (skip-chars-forward "^?#")
	    ;; 3.4. Query
	    (when (looking-at "\\?")
	      (skip-chars-forward "^#"))
	    (setq file (buffer-substring save-pos (point)))
	    ;; 3.5 Fragment
	    (when (looking-at "#")
	      (let ((opoint (point)))
		(forward-char 1)
                (setq fragment (buffer-substring (point) (point-max)))
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
`postgresql://[userspec@][hostspec][/dbname][?paramspec]'. `userspec' is of the form
username:password. If hostspec is a string representing a local path (e.g.
`%2Fvar%2Flib%2Fpostgresql' with percent-encoding) then it is interpreted as a Unix pathname used
for a local Unix domain connection. We do not support all the paramspec keywords supported by libpq,
such as those which specify particular aspects of the TCP connection to PostgreSQL (e.g.
keepalives_interval). The supported paramspec keywords are sslmode (partial support) and
application_name."
  (let* ((parsed (pg-parse-url uri))
         (scheme (url-type parsed)))
    (unless (or (string= "postgres" scheme)
                (string= "postgresql" scheme))
      (signal 'pg-error '("Invalid protocol in connection URI")))
    ;; FIXME unfortunately the url-host is being downcased by url-generic-parse-url, which is
    ;; incorrect when the hostname is specifying a local path.
    (let* ((host (url-unhex-string (url-host parsed)))
           (user (url-user parsed))
           (password (url-password parsed))
           (port (or (url-portspec parsed) 5432))
           (path-query (url-path-and-query parsed))
           (dbname (if (car path-query)
                       ;; ignore the "/" prefix
                       (substring (car path-query) 1)
                     (signal 'pg-error '("Missing database name in connection URI"))))
           (params (cdr path-query))
           ;; this is returning a list of lists, not an alist
           (params (and params (url-parse-query-string params)))
           (sslmode (cadr (assoc "sslmode" params)))
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
           (pg-application-name (or (cadr (assoc "application_name" params))
                                    pg-application-name)))
      ;; If the host is empty or looks like an absolute pathname, connect over Unix-domain socket.
      (if (or (zerop (length host))
              (eq ?/ (aref host 0)))
          (pg-connect-local host dbname user password)
        (pg-connect dbname user password host port tls)))))


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
A handler takes two arguments: the channel and the payload. These correspond to SQL-level
NOTIFY channel, \\='payload\\='."
  (with-current-buffer (process-buffer (pgcon-process con))
    (push handler pgcon--notification-handlers)))

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
    ;; (message "pg-exec: %s" sql)
    (when (> (length encoded) pg--MAX_MESSAGE_LEN)
      (let ((msg (format "SQL statement too long: %s" sql)))
        (signal 'pg-error (list msg))))
    (pg-send-char con ?Q)
    (pg-send-uint con (+ 4 (length encoded) 1) 4)
    (pg-send-string con encoded)
    (pg-flush con)
    (cl-loop for c = (pg-read-char con) do
       ;; (message "pg-exec message-type = %c" c)
       (cl-case c
            ;; NoData
            (?n
             (pg-read-net-int con 4))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg-read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg-read-int con 4))
                    (channel (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (payload (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; Bind -- should not receive this here
            (?B
             (unless attributes
               (signal 'pg-protocol-error (list "Tuple received before metadata")))
             (let ((_msglen (pg-read-net-int con 4)))
               (push (pg-read-tuple con attributes) tuples)))

            ;; CommandComplete -- one SQL command has completed
            (?C
             (let* ((msglen (pg-read-net-int con 4))
                    (msg (pg-read-chars con (- msglen 5)))
                    (_null (pg-read-char con)))
               (setf (pgresult-status result) msg)))
               ;; now wait for the ReadyForQuery message

            ;; DataRow
            (?D
             (let ((_msglen (pg-read-net-int con 4)))
               (push (pg-read-tuple con attributes) tuples)))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con))

            ;; EmptyQueryResponse -- response to an empty query string
            (?I
             (pg-read-net-int con 4)
             (setf (pgresult-status result) "EMPTY"))

            ;; BackendKeyData
            (?K
             (let ((_msglen (pg-read-net-int con 4)))
               (setf (pgcon-pid con) (pg-read-net-int con 4))
               (setf (pgcon-secret con) (pg-read-net-int con 4))))

            ;; NoticeResponse
            (?N
             ;; a Notice response has the same structure and fields as an ErrorResponse
             (let ((notice (pg-read-error-response con)))
               (dolist (handler pg-handle-notice-functions)
                 (funcall handler notice))))

            ;; CursorResponse
            (?P
             (let ((portal (pg-read-string con pg--MAX_MESSAGE_LEN)))
               (setf (pgresult-portal result) portal)))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg-read-net-int con 4))
                    (msg (pg-read-chars con (- msglen 4)))
                    (items (split-string msg (string 0))))
               ;; ParameterStatus items sent by the backend include application_name,
               ;; DateStyle, TimeZone, in_hot_standby, integer_datetimes
               (when (> (length (cl-first items)) 0)
                 (dolist (handler pg-parameter-change-functions)
                   (funcall handler con (cl-first items) (cl-second items))))))

            ;; RowDescription
            (?T
             (when attributes
               (signal 'pg-protocol-error (list "Cannot handle multiple result group")))
             (setq attributes (pg-read-attributes con)))

            ;; CopyFail
            (?f
             (let* ((msglen (pg-read-net-int con 4))
                    (msg (pg-read-chars con (- msglen 4))))
               (message "Unexpected CopyFail message %s" msg)))

            ;; ParseComplete -- not expecting this using the simple query protocol
            (?1
             (pg-read-net-int con 4))

            ;; BindComplete -- not expecting this using the simple query protocol
            (?2
             (pg-read-net-int con 4))

            ;; CloseComplete -- not expecting this using the simple query protocol
            (?3
             (pg-read-net-int con 4))

            ;; PortalSuspended -- this message is not expected using the simple query protocol
            (?s
             (message "Unexpected PortalSuspended message in pg-exec (sql was %s)" sql)
             (pg-read-net-int con 4)
             (setf (pgresult-incomplete result) t)
             (setf (pgresult-tuples result) (nreverse tuples))
             (setf (pgresult-status result) "SUSPENDED")
             (pg-connection-set-busy con nil)
             (cl-return-from pg-exec result))

            ;; ReadyForQuery
            (?Z
             (let ((_msglen (pg-read-net-int con 4))
                   (status (pg-read-char con)))
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
                      (signal 'pg-error (list msg)))))
           (tuples (pgresult-tuples result)))
       (nth which tuples)))
    (:oid
     (let ((status (pgresult-status result)))
       (if (string= "INSERT" (substring status 0 6))
           (string-to-number (substring status 7 (cl-position ? status :start 7)))
         (let ((msg (format "Only INSERT commands generate an oid: %s" status)))
           (signal 'pg-error (list msg))))))
    (t
     (let ((msg (format "Unknown result request %s" what)))
       (signal 'pg-error (list msg))))))


;; Similar to libpq function PQescapeIdentifier.
;; See https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-EXEC-ESCAPE-STRING
;;
;; This function can help to prevent SQL injection attacks ("Little Bobby Tables",
;; https://xkcd.com/327/) in situations where you can't use a prepared statement (a parameterized
;; query, using the prepare/bind/execute extended query message flow in PostgreSQL). You might need
;; this for example when specifying the name of a column in a SELECT statement. See function
;; `pg-exec-prepared' which should be used when possible instead of relying on this function.
(defun pg-escape-identifier (str)
  "Escape an SQL identifier, such as a table, column, or function name.
Similar to libpq function PQescapeIdentifier.
You should use prepared statements (`pg-exec-prepared') instead of this function whenever possible."
  (with-temp-buffer
    (insert ?\")
    (cl-loop for c across str
             do (when (eql c ?\") (insert ?\"))
             (insert c))
    (insert ?\")
    (buffer-string)))

(defun pg-escape-literal (str)
  "Escape a string for use within an SQL command.
Similar to libpq function PQescapeLiteral.
You should use prepared statements (`pg-exec-prepared') instead of this function whenever possible."
  (with-temp-buffer
    (insert ?E)
    (insert ?\')
    (cl-loop
     for c across str do
     (when (eql c ?\') (insert ?\'))
     (when (eql c ?\\) (insert ?\\))
     (insert c))
    (insert ?\')
    (buffer-string)))


(defun pg--lookup-oid (type-name)
  (or (gethash type-name pg--type-oid)
      (signal 'pg-error (list (format "Undefined PostgreSQL type %s" type-name)))))

(defun pg--lookup-type-name (oid)
  (or (gethash oid pg--type-name)
      (signal 'pg-error (list (format "Unknown PostgreSQL oid %d" oid)))))


(cl-defun pg-prepare (con query argument-types &key (name ""))
  "Prepare statement QUERY with ARGUMENT-TYPES on connection CON.
The prepared statement may be given optional NAME (defaults to an
unnamed prepared statement). ARGUMENT-TYPES is a list of
PostgreSQL type names of the form (\"int4\" \"text\" \"bool\")."
  (let* ((ce (pgcon-client-encoding con))
         (query/enc (if ce (encode-coding-string query ce t) query))
         (oids (mapcar #'pg--lookup-oid argument-types))
         (len (+ 4 (1+ (length name)) (1+ (length query/enc)) 2 (* 4 (length oids)))))
    ;; send a Parse message
    (pg-connection-set-busy con t)
    (pg-send-char con ?P)
    (pg-send-uint con len 4)
    (pg-send-string con name)
    (pg-send-string con query/enc)
    (pg-send-uint con (length oids) 2)
    (dolist (oid oids)
      (pg-send-uint con oid 4)))
  name)

(cl-defun pg-bind (con statement-name typed-arguments &key (portal ""))
  "Bind the SQL prepared statement STATEMENT-NAME to arguments TYPED-ARGUMENTS.
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
           collect (if serializer
                       ;; this argument will be sent in binary format
                       (cons (funcall serializer v) 1)
                     ;; this argument will be sent in text format
                     (let* ((raw (if (stringp v) v (format "%s" v)))
                            (encoded (if ce (encode-coding-string raw ce t) raw)))
                       (cons encoded 0)))))
         (len (+ 4
                 (1+ (length portal))
                 (1+ (length statement-name))
                 2
                 (* 2 (length argument-types))
                 2
                 (cl-loop for v in (mapcar #'car serialized-values) sum (+ 4 (length v)))
                 2)))
    ;; send a Bind message
    (pg-send-char con ?B)
    (pg-send-uint con len 4)
    ;; the destination portal
    (pg-send-string con portal)
    (pg-send-string con statement-name)
    (pg-send-uint con (length argument-types) 2)
    (cl-loop for (_ . binary-p) in serialized-values
             do (pg-send-uint con binary-p 2))
    (pg-send-uint con (length argument-values) 2)
    (cl-loop
     for (v . _) in serialized-values
     do (if (null v)
            ;; for a null value, send -1 followed by zero octets for the value
            (pg-send-uint con -1 4)
          (pg-send-uint con (length v) 4)
          (pg-send-octets con v)))
    ;; the number of result-column format codes: we use zero to indicate that result columns can use
    ;; text format
    (pg-send-uint con 0 2)
    portal))

(defun pg-describe-portal (con portal)
  (let ((len (+ 4 1 (1+ (length portal)))))
    ;; send a Describe message for this portal
    (pg-send-char con ?D)
    (pg-send-uint con len 4)
    (pg-send-char con ?P)
    (pg-send-string con portal)))

(cl-defun pg-execute (con portal &key (max-rows 0))
  (let* ((ce (pgcon-client-encoding con))
         (pn/encoded (if ce (encode-coding-string portal ce t) portal))
         (len (+ 4 (1+ (length pn/encoded)) 4)))
    ;; send an Execute message
    (pg-send-char con ?E)
    (pg-send-uint con len 4)
    ;; the destination portal
    (pg-send-string con pn/encoded)
    ;; Maximum number of rows to return; zero means "no limit"
    (pg-send-uint con max-rows 4)))

(cl-defun pg-fetch (con result &key (max-rows 0))
  "Fetch pending rows from portal in RESULT on database connection CON.
Retrieve at most MAX-ROWS rows (default value of zero means no limit).
Returns a pgresult structure (see function `pg-result')."
  (let* ((tuples (list))
         (attributes (pgresult-attributes result)))
    (setf (pgresult-status result) nil)
    ;; We are counting on the Describe message having been sent prior to calling pg-fetch
    (pg-execute con (pgresult-portal result) :max-rows max-rows)
    ;; If we are requesting a subset of available rows, we send a Flush message instead of a Sync
    ;; message, otherwise our unnamed portal will be closed by the Sync message and we won't be able
    ;; to retrieve more rows on the next call to pg-fetch.
    (cond ((zerop max-rows)
           ;; send a Sync message
           (pg-send-char con ?S)
           (pg-send-uint con 4 4))
          (t
           ;; send a Flush message
           (pg-send-char con ?H)
           (pg-send-uint con 4 4)))
    (pg-flush con)
    (cl-loop
     for c = (pg-read-char con) do
     ;; (message "pg-fetch got %c" c)
     (cl-case c
       ;; ParseComplete
       (?1
        (pg-read-net-int con 4))

       ;; BindComplete
       (?2
        (pg-read-net-int con 4))

       ;; RowDescription
       (?T
        (when attributes
          (signal 'pg-protocol-error (list "Cannot handle multiple result group")))
        (setq attributes (pg-read-attributes con))
        (setf (pgresult-attributes result) attributes))

       ;; DataRow message
       (?D
        (let ((_msglen (pg-read-net-int con 4)))
          (push (pg-read-tuple con attributes) tuples)))

       ;; PortalSuspended -- the row-count limit for the Execute message was reached; more data is
       ;; available with another Execute message.
       (?s
        (unless (> max-rows 0)
          (message "Unexpected PortalSuspended message in pg-exec-prepared"))
        (pg-read-net-int con 4)
        (setf (pgresult-incomplete result) t)
        (setf (pgresult-tuples result) (nreverse tuples))
        (setf (pgresult-status result) "SUSPENDED")
        (pg-connection-set-busy con nil)
        (cl-return-from pg-fetch result))

       ;; CommandComplete -- one SQL command has completed (portal's execution is completed)
       (?C
        (let* ((msglen (pg-read-net-int con 4))
               (msg (pg-read-chars con (- msglen 5)))
               (_null (pg-read-char con)))
          (setf (pgresult-status result) msg))
        (setf (pgresult-incomplete result) nil)
        (when (> max-rows 0)
          ;; send a Sync message to close the portal and request the ReadyForQuery
          (pg-send-char con ?S)
          (pg-send-uint con 4 4)
          (pg-flush con)))

       ;; EmptyQueryResponse -- the response to an empty query string
       (?I
        (pg-read-net-int con 4)
        (setf (pgresult-status result) "EMPTY"))

       ;; NoData message
       (?n
        (pg-read-net-int con 4))

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
        (let ((_msglen (pg-read-net-int con 4))
              (status (pg-read-char con)))
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
  (let* ((argument-types (mapcar #'cdr typed-arguments))
         (ps-name (pg-prepare con query argument-types))
         (portal-name (pg-bind con ps-name typed-arguments :portal portal))
         (result (make-pgresult :connection con :portal portal-name)))
    (pg-describe-portal con portal-name)
    (pg-fetch con result :max-rows max-rows)))

(cl-defun pg-close-portal (con portal)
  "Close the portal named PORTAL that was opened by pg-exec-prepared."
  (let ((len (+ 4 1 (1+ (length portal)))))
    ;; send a Close message
    (pg-send-char con ?C)
    (pg-send-uint con len 4)
    (pg-send-char con ?P)
    (pg-send-string con portal)
    ;; send a Sync message
    (pg-send-char con ?S)
    (pg-send-uint con 4 4)
    (pg-flush con)
    (cl-loop
     for c = (pg-read-char con) do
     (cl-case c
       ;; ParseComplete
       (?1
        (pg-read-net-int con 4))

       ;; CloseComplete
       (?3
        (pg-read-net-int con 4))

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
        (let ((_msglen (pg-read-net-int con 4))
              (status (pg-read-char con)))
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
    (signal 'pg-error (list "Invalid COPY query")))
  (unless (cl-search "FROM STDIN" query)
    (signal 'pg-error (list "COPY command must contain 'FROM STDIN'")))
  (pg-connection-set-busy con t)
  (let ((result (make-pgresult :connection con))
        (ce (pgcon-client-encoding con)))
    (pg-send-char con ?Q)
    (pg-send-uint con (+ 4 (length query) 1) 4)
    (pg-send-string con query)
    (pg-flush con)
    (let ((more-pending t))
      (while more-pending
        (let ((c (pg-read-char con)))
          (cl-case c
            (?G
             ;; CopyInResponse
             (let ((_msglen (pg-read-net-int con 4))
                   (status (pg-read-net-int con 1))
                   (cols (pg-read-net-int con 2))
                   (format-codes (list)))
               ;; status=0, which will be returned by recent backend versions: the backend is
               ;; expecting data in textual format (rows separated by newlines, columns separated by
               ;; separator characters, etc.).
               ;;
               ;; status=1: the backend is expecting binary format (which is similar to DataRow
               ;; format, and which we don't implement here).
               (dotimes (_c cols)
                 (push (pg-read-net-int con 2) format-codes))
               (unless (zerop status)
                 (signal 'pg-error (list "BINARY format for COPY is not implemented")))
               (setq more-pending nil)))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg-read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg-read-int con 4))
                    (channel (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (payload (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg-read-net-int con 4))
                    (msg (pg-read-chars con (- msglen 4)))
                    (items (split-string msg (string 0))))
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
            (pg-send-char con ?d)
            (pg-send-uint con (+ 4 (length encoded)) 4)
            (pg-send-octets con encoded)))))
    ;; send a CopyDone message
    (pg-send-char con ?c)
    (pg-send-uint con 4 4)
    (pg-flush con)
    ;; Backend sends us either CopyDone or CopyFail, followed by CommandComplete + ReadyForQuery
    (cl-loop
     for c = (pg-read-char con) do
     (cl-case c
       (?c
        ;; CopyDone
        (let ((_msglen (pg-read-net-int con 4)))
          nil))

       ;; CopyFail
       (?f
        (let* ((msglen (pg-read-net-int con 4))
               (msg (pg-read-chars con (- msglen 4)))
               (emsg (format "COPY failed: %s" msg)))
          (signal 'pg-copy-failed (list emsg))))

       ;; CommandComplete -- SQL command has completed. After this we expect a ReadyForQuery message.
       (?C
        (let* ((msglen (pg-read-net-int con 4))
               (msg (pg-read-chars con (- msglen 5)))
               (_null (pg-read-char con)))
          (setf (pgresult-status result) msg)))

       ;; NotificationResponse
       (?A
        (let* ((_msglen (pg-read-net-int con 4))
               ;; PID of the notifying backend
               (_pid (pg-read-int con 4))
               (channel (pg-read-string con pg--MAX_MESSAGE_LEN))
               (payload (pg-read-string con pg--MAX_MESSAGE_LEN))
               (buf (process-buffer (pgcon-process con)))
               (handlers (with-current-buffer buf pgcon--notification-handlers)))
          (dolist (handler handlers)
            (funcall handler channel payload))))

       ;; ErrorResponse
       (?E
        (pg-handle-error-response con))

       ;; ReadyForQuery message
       (?Z
        (let ((_msglen (pg-read-net-int con 4))
              (status (pg-read-char con)))
          (when (eql ?E status)
            (message "PostgreSQL ReadyForQuery message with error status"))
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
can be decoded using `pg-result'."
  (unless (string-equal "COPY" (upcase (cl-subseq query 0 4)))
    (signal 'pg-error (list "Invalid COPY query")))
  (unless (cl-search "TO STDOUT" query)
    (signal 'pg-error (list "COPY command must contain 'TO STDOUT'")))
  (pg-connection-set-busy con t)
  (let ((result (make-pgresult :connection con)))
    (pg-send-char con ?Q)
    (pg-send-uint con (+ 4 (length query) 1) 4)
    (pg-send-string con query)
    (pg-flush con)
    (let ((more-pending t))
      (while more-pending
        (let ((c (pg-read-char con)))
          (cl-case c
            ;; CopyOutResponse
            (?H
             (let ((_msglen (pg-read-net-int con 4))
                   (status (pg-read-net-int con 1))
                   (cols (pg-read-net-int con 2))
                   (format-codes (list)))
               ;; status=0 indicates the overall COPY format is textual (rows separated by
               ;; newlines, columns separated by separator characters, etc.). 1 indicates the
               ;; overall copy format is binary (which we don't implement here).
               (dotimes (_c cols)
                 (push (pg-read-net-int con 2) format-codes))
               (unless (zerop status)
                 (signal 'pg-error (list "BINARY format for COPY is not implemented")))
               (setq more-pending nil)))

            ;; NotificationResponse
            (?A
             (let* ((_msglen (pg-read-net-int con 4))
                    ;; PID of the notifying backend
                    (_pid (pg-read-int con 4))
                    (channel (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (payload (pg-read-string con pg--MAX_MESSAGE_LEN))
                    (buf (process-buffer (pgcon-process con)))
                    (handlers (with-current-buffer buf pgcon--notification-handlers)))
               (dolist (handler handlers)
                 (funcall handler channel payload))))

            ;; ErrorResponse
            (?E
             (pg-handle-error-response con))

            ;; ParameterStatus sent in response to a user update over the connection
            (?S
             (let* ((msglen (pg-read-net-int con 4))
                    (msg (pg-read-chars con (- msglen 4)))
                    (items (split-string msg (string 0))))
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
       for c = (pg-read-char con) do
       (cl-case c
         ;; CopyData
         (?d
          (let* ((msglen (pg-read-net-int con 4))
                 (payload (pg-read-chars-old con (- msglen 4)))
                 (ce (pgcon-client-encoding con))
                 (decoded (if ce (decode-coding-string payload ce t) payload)))
            (insert decoded)))

         ;; CopyDone
         (?c
          (let ((_msglen (pg-read-net-int con 4)))
            nil))

         ;; CopyFail
         (?f
          (let* ((msglen (pg-read-net-int con 4))
                 (msg (pg-read-chars con (- msglen 4)))
                 (emsg (format "COPY failed: %s" msg)))
            (signal 'pg-copy-failed (list emsg))))

         ;; CommandComplete -- SQL command has completed. After this we expect a ReadyForQuery message.
         (?C
          (let* ((msglen (pg-read-net-int con 4))
                 (msg (pg-read-chars con (- msglen 5)))
                 (_null (pg-read-char con)))
            (setf (pgresult-status result) msg)))

         ;; NotificationResponse
         (?A
          (let* ((_msglen (pg-read-net-int con 4))
                 ;; PID of the notifying backend
                 (_pid (pg-read-int con 4))
                 (channel (pg-read-string con pg--MAX_MESSAGE_LEN))
                 (payload (pg-read-string con pg--MAX_MESSAGE_LEN))
                 (buf (process-buffer (pgcon-process con)))
                 (handlers (with-current-buffer buf pgcon--notification-handlers)))
            (dolist (handler handlers)
              (funcall handler channel payload))))

         ;; ErrorResponse
         (?E
          (pg-handle-error-response con))

         ;; ReadyForQuery message
         (?Z
          (let ((_msglen (pg-read-net-int con 4))
                (status (pg-read-char con)))
            (when (eql ?E status)
              (message "PostgreSQL ReadyForQuery message with error status"))
            (pg-connection-set-busy con nil)
            (cl-return-from pg-copy-to-buffer result)))

         (t
          (let ((msg (format "Unknown response type from backend in copy-to-buffer/2: %s" c)))
            (signal 'pg-protocol-error (list msg)))))))))


(defun pg-sync (con)
  (pg-connection-set-busy con t)
  (pg-send-char con ?S)
  (pg-send-uint con 4 4)
  (pg-flush con)
  (when (fboundp 'thread-yield)
    (thread-yield))
  ;; discard any content in our process buffer
  (with-current-buffer (process-buffer (pgcon-process con))
    (setq-local pgcon--position (point-max)))
  (pg-connection-set-busy con nil))


(defun pg-cancel (con)
  "Cancel the command currently being processed by the backend.
The cancellation request concerns the command requested over connection CON."
  ;; Send a CancelRequest message. We open a new connection to the server and
  ;; send the CancelRequest message, rather than the StartupMessage message that
  ;; would ordinarily be sent across a new connection. The server will process
  ;; this request and then close the connection.
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
    (pg-send-uint ccon 16 4)
    (pg-send-uint ccon 80877102 4)
    (pg-send-uint ccon (pgcon-pid con) 4)
    (pg-send-uint ccon (pgcon-secret con) 4)
    (pg-disconnect ccon)))

(defun pg-disconnect (con)
  "Close the PostgreSQL connection CON.
This command should be used when you have finished with the database.
It will release memory used to buffer the data transfered between
PostgreSQL and Emacs. CON should no longer be used."
  ;; send a Terminate message
  (pg-connection-set-busy con t)
  (pg-send-char con ?X)
  (pg-send-uint con 4 4)
  (pg-flush con)
  (delete-process (pgcon-process con))
  (kill-buffer (process-buffer (pgcon-process con))))


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

(defun pg-initialize-parsers (con)
  "Initialize the datatype parsers on PostgreSQL connection CON."
  ;; FIXME this query is retrieving more oids that we we really need
  (let* ((pgtypes (pg-exec con "SELECT typname,oid FROM pg_type"))
         (tuples (pg-result pgtypes :tuples)))
    (dolist (tuple tuples)
      (let* ((typname (cl-first tuple))
             ;; we need to parse this explicitly, because the value parsing infrastructure is not
             ;; yet set up
             (oid (string-to-number (cl-second tuple)))
             (parser (gethash typname pg--parsers-name)))
        (puthash oid typname pg--type-name)
        (puthash typname oid pg--type-oid)
        (when parser
          (puthash oid parser pg--parsers-oid))))))

(defun pg-parse (str oid encoding)
  (let ((parser (gethash oid pg--parsers-oid)))
    (if parser
        (funcall parser str encoding)
      str)))

;; Map between PostgreSQL names for encodings and their Emacs name.
;; For Emacs, see coding-system-alist.
(defconst pg--encoding-names
  '(("UTF8"    . utf-8)
    ("UTF16"   . utf-16)
    ("LATIN1"  . latin-1)
    ("LATIN2"  . latin-2)
    ("LATIN3"  . latin-3)
    ("LATIN4"  . latin-4)
    ("LATIN5"  . latin-5)
    ("LATIN6"  . latin-6)
    ("LATIN7"  . latin-7)
    ("LATIN8"  . latin-8)
    ("LATIN9"  . latin-9)
    ("LATIN10" . latin-10)
    ("WIN1250" . windows-1250)
    ("WIN1251" . windows-1251)
    ("WIN1252" . windows-1252)
    ("WIN1253" . windows-1253)
    ("WIN1254" . windows-1254)
    ("WIN1255" . windows-1255)
    ("WIN1256" . windows-1256)
    ("WIN1257" . windows-1257)
    ("WIN1258" . windows-1258)
    ("SHIFT_JIS_2004" . shift_jis-2004)
    ("SJIS"    . shift_jis-2004)
    ("GB18030" . gb18030)
    ("EUC_TW"  . euc-taiwan)
    ("EUC_KR"  . euc-korea)
    ("EUC_JP"  . euc-japan)
    ("EUC_CN"  . euc-china)
    ("BIG5"    . big5)))

(defun pg-normalize-encoding-name (name)
  "Convert PostgreSQL encoding NAME to an Emacs encoding name."
  (let ((m (assoc name pg--encoding-names #'string=)))
    (when m (cdr m))))


(defun pg-register-parser (type-name parser)
  (puthash type-name parser pg--parsers-name))
(put 'pg-register-parser 'lisp-indent-function 'defun)

(defun pg-lookup-parser (type-name)
  (gethash type-name pg--parsers-name))

(defun pg-bool-parser (str _encoding)
  (cond ((string= "t" str) t)
        ((string= "f" str) nil)
        (t (let ((msg (format "Badly formed boolean from backend: %s" str)))
             (signal 'pg-protocol-error (list msg))))))

(pg-register-parser "bool" #'pg-bool-parser)

(defun pg-bit-parser (str _encoding)
  "Parse PostgreSQL value STR as a bit."
  (let* ((len (length str))
         (bv (make-bool-vector len t)))
    (dotimes (i len)
      (setf (aref bv i) (eql ?1 (aref str i))))
    bv))

(pg-register-parser "bit" #'pg-bit-parser)
(pg-register-parser "varbit" #'pg-bit-parser)

(defun pg-char-parser (str _encoding)
  (aref str 0))

(pg-register-parser "char" #'pg-char-parser)
(pg-register-parser "bpchar" #'pg-char-parser)

(defun pg-text-parser (str encoding)
  "Parse PostgreSQL value STR as text."
  (if encoding
      (decode-coding-string str encoding)
    str))

(pg-register-parser "char2" #'pg-text-parser)
(pg-register-parser "char4" #'pg-text-parser)
(pg-register-parser "char8" #'pg-text-parser)
(pg-register-parser "char16" #'pg-text-parser)
(pg-register-parser "name" #'pg-text-parser)
(pg-register-parser "text" #'pg-text-parser)
(pg-register-parser "varchar" #'pg-text-parser)

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

;; This function must be called before using the HSTORE extension. It loads the extension if
;; necessary, and sets up the parsing support for HSTORE datatypes. This is necessary because
;; the hstore type is not defined on startup in the pg_type table.
(defun pg-hstore-setup (con)
  "Prepare for using and parsing HSTORE datatypes on PostgreSQL connection CON.
Return nil if the extension could not be loaded."
  (when (condition-case nil
            (pg-exec con "CREATE EXTENSION IF NOT EXISTS hstore")
          (pg-error nil))
    (let* ((res (pg-exec con "SELECT oid FROM pg_type WHERE typname='hstore'"))
           (oid (car (pg-result res :tuple 0)))
           (parser (pg-lookup-parser "hstore")))
      (when parser
        (puthash oid parser pg--parsers-oid)))))

;; Note however that the hstore type is generally not present in the pg_types table
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
  (string-to-number str))

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
        (t
         (string-to-number str))))

(pg-register-parser "numeric" #'pg-float-parser)
(pg-register-parser "float4" #'pg-float-parser)
(pg-register-parser "float8" #'pg-float-parser)

;; FIXME we are not currently handling multidimensional arrays correctly. They are serialized by
;; PostgreSQL using the same typid as a unidimensional array, with only the presence of additional
;; levels of {} marking the extra dimensions.
;; See https://www.postgresql.org/docs/current/arrays.html

(defun pg-intarray-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of integers."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for int array")))
    (let ((segments (split-string (cl-subseq str 1 (- len 1)) ",")))
      (apply #'vector (mapcar #'string-to-number segments)))))

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
    (let ((segments (split-string (cl-subseq str 1 (- len 1)) ",")))
      (apply #'vector (mapcar (lambda (x) (pg-float-parser x nil)) segments)))))

(pg-register-parser "_float4" #'pg-floatarray-parser)
(pg-register-parser "_float8" #'pg-floatarray-parser)
(pg-register-parser "_numeric" #'pg-floatarray-parser)

(defun pg-boolarray-parser (str _encoding)
  "Parse PostgreSQL value STR as an array of boolean values."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for bool array")))
    (let ((segments (split-string (cl-subseq str 1 (1- len)) ",")))
      (apply #'vector (mapcar (lambda (x) (pg-bool-parser x nil)) segments)))))

(pg-register-parser "_bool" #'pg-boolarray-parser)

(defun pg-chararray-parser (str encoding)
  "Parse PostgreSQL value STR as an array of characters."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for char array")))
    (let ((segments (split-string (cl-subseq str 1 (1- len)) ",")))
      (apply #'vector (mapcar (lambda (x) (pg-char-parser x encoding)) segments)))))

(pg-register-parser "_char" #'pg-chararray-parser)
(pg-register-parser "_bpchar" #'pg-chararray-parser)

(defun pg-textarray-parser (str encoding)
  "Parse PostgreSQL value STR as an array of TEXT values."
  (let ((len (length str)))
    (unless (and (eql (aref str 0) ?{)
                 (eql (aref str (1- len)) ?}))
      (signal 'pg-protocol-error (list "Unexpected format for text array")))
    (let ((segments (split-string (cl-subseq str 1 (1- len)) ",")))
      (apply #'vector (mapcar (lambda (x) (pg-text-parser x encoding)) segments)))))

(pg-register-parser "_text" #'pg-textarray-parser)

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

;; format for ISO dates is "1999-10-24"
(defun pg-date-parser (str _encoding)
  "Parse PostgreSQL value STR as a date."
  (let ((year  (string-to-number (substring str 0 4)))
        (month (string-to-number (substring str 5 7)))
        (day   (string-to-number (substring str 8 10))))
    (encode-time 0 0 0 day month year)))

(pg-register-parser "date" #'pg-date-parser)

(defconst pg--ISODATE_REGEX
  (concat "\\([0-9]+\\)-\\([0-9][0-9]\\)-\\([0-9][0-9]\\) " ; Y-M-D
          "\\([0-9][0-9]\\):\\([0-9][0-9]\\):\\([.0-9]+\\)" ; H:M:S.S
          "\\([-+][0-9]+\\)?")) ; TZ

;;  format for abstime/timestamp etc with ISO output syntax is
;;;    "1999-01-02 14:32:53+01"
;; which we convert to the internal Emacs date/time representation
;; (there may be a fractional seconds quantity as well, which the regex
;; handles)
(defun pg-isodate-parser (str _encoding)
  "Parse PostgreSQL value STR as an ISO-formatted date."
  (if (string-match pg--ISODATE_REGEX str)  ; is non-null
      (let ((year    (string-to-number (match-string 1 str)))
            (month   (string-to-number (match-string 2 str)))
            (day     (string-to-number (match-string 3 str)))
            (hours   (string-to-number (match-string 4 str)))
            (minutes (string-to-number (match-string 5 str)))
            (seconds (round (string-to-number (match-string 6 str))))
            (tz      (string-to-number (or (match-string 7 str) "0"))))
        (encode-time seconds minutes hours day month year (* 3600 tz)))
    (let ((msg (format "Badly formed ISO timestamp from backend: %s" str)))
      (signal 'pg-protocol-error (list msg)))))

(pg-register-parser "timestamp"  #'pg-isodate-parser)
(pg-register-parser "timestamptz" #'pg-isodate-parser)
(pg-register-parser "datetime" #'pg-isodate-parser)

(pg-register-parser "time" #'pg-text-parser)     ; preparsed "15:32:45"
(pg-register-parser "reltime" #'pg-text-parser)     ; don't know how to parse these
(pg-register-parser "timespan" #'pg-text-parser)
(pg-register-parser "tinterval" #'pg-text-parser)


;;; Support for the pgvector extension (vector similarity search).

;; This function must be called before using the pgvector extension. It loads the extension if
;; necessary, and sets up the parsing support for vector datatypes.
(defun pg-vector-setup (con)
  "Prepare for using and parsing VECTOR datatypes on PostgreSQL connection CON.
Return nil if the extension could not be set up."
  (when (condition-case nil
            (pg-exec con "CREATE EXTENSION IF NOT EXISTS vector")
          (pg-error nil))
    (let* ((res (pg-exec con "SELECT oid FROM pg_type WHERE typname='vector'"))
           (oid (car (pg-result res :tuple 0)))
           (parser (pg-lookup-parser "vector")))
      (when parser
        (puthash oid parser pg--parsers-oid)))))

;; pgvector embeddings are sent by the database as strings, in the form "[1,2,3]". We don't need to
;; define a serialization function because we send the embeddings a strings.
(pg-register-parser "vector"
  (lambda (s _e)
    (let ((len (length s)))
      (unless (and (eql (aref s 0) ?\[)
                   (eql (aref s (1- len)) ?\]))
        (signal 'pg-protocol-error (list "Unexpected format for VECTOR embedding")))
      (let ((segments (split-string (cl-subseq s 1 (1- len)) ",")))
        (apply #'vector (mapcar #'string-to-number segments))))))


;; We don't register a serializer for "text" and "varchar", because they are sent in text mode, and
;; therefore correctly encoded according to the connection encoding.
(defun pg-register-serializer (type-name serializer)
  (puthash type-name serializer pg--serializers))
(put 'pg-register-serializer 'lisp-indent-function 'defun)

;; (pg-register-serializer "text" #'identity)
;; (pg-register-serializer "varchar" #'identity)
(pg-register-serializer "bytea" #'identity)
(pg-register-serializer "jsonb" #'identity)

(pg-register-serializer "bool" (lambda (v) (if v (string 1) (string 0))))

;; for the bit type, use text serialization

(pg-register-serializer "char"
  (lambda (v)
    (cl-assert (<= 0 v 255))
    (string v)))

(pg-register-serializer "bpchar"
  (lambda (v)
    (cl-assert (<= 0 v 255))
    (string v)))

;; see https://www.postgresql.org/docs/current/datatype-numeric.html
(pg-register-serializer "int2"
  (lambda (v)
    (cl-assert (integerp v))
    (cl-assert (<= (- (expt 2 15)) v (expt 2 15)))
    ;; This use of bindat-type makes us depend on Emacs 28.1, released in April 2022.
    (bindat-pack (bindat-type sint 16 nil) v)))

(pg-register-serializer "smallint"
  (lambda (v)
    (cl-assert (integerp v))
    (cl-assert (<= (- (expt 2 15)) v (expt 2 15)))
    (bindat-pack (bindat-type sint 16 nil) v)))

(pg-register-serializer "int4"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type sint 32 nil) v)))

(pg-register-serializer "integer"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type sint 32 nil) v)))

;; see https://www.postgresql.org/docs/current/datatype-oid.html
(pg-register-serializer "oid"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type uint 32 nil) v)))

(pg-register-serializer "int8"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type sint 64 nil) v)))

(pg-register-serializer "bigint"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type sint 64 nil) v)))

(pg-register-serializer "smallserial"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type uint 16 nil) v)))

(pg-register-serializer "serial"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type uint 32 nil) v)))

(pg-register-serializer "bigserial"
  (lambda (v)
    (cl-assert (integerp v))
    (bindat-pack (bindat-type uint 64 nil) v)))

;; for float4 and float8, we don't know how to access the binary representation from Emacs Lisp. 
;; here a possible conversion routine
;; https://lists.gnu.org/archive/html/help-gnu-emacs/2002-10/msg00724.html

(if (fboundp 'json-serialize)
    (pg-register-serializer "json" #'json-serialize)
  (require 'json)
  (pg-register-serializer "json" #'json-encode))


;; pwdhash = md5(password + username).hexdigest()
;; hash = ′md5′ + md5(pwdhash + salt).hexdigest()
(defun pg-do-md5-authentication (con user password)
  "Attempt MD5 authentication with PostgreSQL database over connection CON.
Authenticate as USER with PASSWORD."
  (let* ((salt (pg-read-chars con 4))
         (pwdhash (md5 (concat password user)))
         (hash (concat "md5" (md5 (concat pwdhash salt)))))
    (pg-send-char con ?p)
    (pg-send-uint con (+ 5 (length hash)) 4)
    (pg-send-string con hash)
    (pg-flush con)))


;; TODO: implement stringprep for user names and passwords, as per RFC4013.
(defun pg-sasl-prep (string)
  string)


(defun pg-logxor-string (s1 s2)
  "Elementwise XOR of each character of strings S1 and S2."
  (let ((len (length s1)))
    (cl-assert (eql len (length s2)))
    (let ((out (make-string len 0)))
      (dotimes (i len)
        (setf (aref out i) (logxor (aref s1 i) (aref s2 i))))
      out)))

;; PBKDF2 is a key derivation function used to reduce vulnerability to brute-force password guessing
;; attempts <https://en.wikipedia.org/wiki/PBKDF2>.
(defun pg-pbkdf2-hash-sha256 (password salt iterations)
  "Return the PBKDF2 hash of PASSWORD using SALT and ITERATIONS."
  (let* ((hash (gnutls-hash-mac 'SHA256 (cl-copy-seq password) (concat salt (string 0 0 0 1))))
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
Authenticate as USER with PASSWORD."
  (let* ((mechanism "SCRAM-SHA-256")
         (client-nonce (or pg--*force-client-nonce*
                           (apply #'string (cl-loop for i below 32 collect (+ ?A (random 25))))))
         (client-first (format "n,,n=%s,r=%s" user client-nonce))
         (len-cf (length client-first))
         ;; packet length doesn't include the initial ?p message type indicator
         (len-packet (+ 4 (1+ (length mechanism)) 4 len-cf)))
    ;; send the SASLInitialResponse message
    (pg-send-char con ?p)
    (pg-send-uint con len-packet 4)
    (pg-send-string con mechanism)
    (pg-send-uint con len-cf 4)
    (pg-send-octets con client-first)
    (pg-flush con)
    (let ((c (pg-read-char con)))
      (cl-case c
        (?E
         ;; an ErrorResponse message
         (pg-handle-error-response con "during SASL auth"))

        ;; AuthenticationSASLContinue message, what we are hoping for
        (?R
         (let* ((len (pg-read-net-int con 4))
                (type (pg-read-net-int con 4))
                (server-first-msg (pg-read-chars con (- len 8))))
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
                  (iterations (string-to-number (substring i= 2)))
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
             (pg-send-char con ?p)
             (pg-send-uint con (+ 4 (length client-final-msg)) 4)
             (pg-send-octets con client-final-msg)
             (pg-flush con)
             (let ((c (pg-read-char con)))
               (cl-case c
                 (?E
                  ;; an ErrorResponse message
                  (pg-handle-error-response con "after SASLResponse"))

                 (?R
                  ;; an AuthenticationSASLFinal message
                  (let* ((len (pg-read-net-int con 4))
                         (type (pg-read-net-int con 4))
                         (server-final-msg (pg-read-chars con (- len 8))))
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
Authenticate as USER with PASSWORD."
  (let ((mechanisms (list)))
    ;; read server's list of preferered authentication mechanisms
    (cl-loop for mech = (pg-read-string con 4096)
             while (not (zerop (length mech)))
             do (push mech mechanisms))
    (if (member "SCRAM-SHA-256" mechanisms)
        (pg-do-scram-sha256-authentication con user password)
      (let ((msg (format "Can't handle any of SASL mechanisms %s" mechanisms)))
        (signal 'pg-protocol-error (list msg))))))



(defun pg-table-owner (con table)
  "Return the owner of TABLE in a PostgreSQL database.
Uses database connection CON."
  (let ((res (pg-exec-prepared con
              "SELECT tableowner FROM pg_catalog.pg_tables WHERE tablename=$1"
              `((,table . "text")))))
    (cl-first (pg-result res :tuple 0))))

;; As per https://www.postgresql.org/docs/current/sql-comment.html
(defun pg-table-comment (con table)
  (let* ((res (pg-exec-prepared con "SELECT obj_description($1::regclass::oid, 'pg_class')"
                                `((,table . "text"))))
         (tuples (pg-result res :tuples)))
    (when tuples
      (caar tuples))))

(gv-define-setter pg-table-comment (comment con table)
  `(let* ((sql (format "COMMENT ON TABLE %s IS %s"
                       (pg-escape-identifier ,table)
                       (pg-escape-literal ,comment))))
     ;; We can't use a prepared statement in this situation.
     (pg-exec ,con sql)
     ,comment))


;; large object support ================================================
;;
;; Humphrey: Who is Large and to what does he object?
;;
;; Large objects are the PostgreSQL way of doing what most databases
;; call BLOBs (binary large objects). In addition to being able to
;; stream data to and from large objects, PostgreSQL's
;; object-relational capabilities allow the user to provide functions
;; which act on the objects.
;;
;; For example, the user can define a new type called "circle", and
;; define a C or Tcl function called `circumference' which will act on
;; circles. There is also an inheritance mechanism in PostgreSQL.
;;
;;======================================================================
(defconst pg--INV_ARCHIVE 65536)         ; fe-lobj.c
(defconst pg--INV_WRITE   131072)
(defconst pg--INV_READ    262144)
(defconst pg--LO_BUFIZE   1024)

(defvar pg-lo-initialized nil)
(defvar pg-lo-functions '())

(defun pg-lo-init (con)
  (let* ((res (pg-exec con
                       "SELECT proname, oid from pg_proc WHERE "
                       "proname = 'lo_open' OR "
                       "proname = 'lo_close' OR "
                       "proname = 'lo_creat' OR "
                       "proname = 'lo_unlink' OR "
                       "proname = 'lo_lseek' OR "
                       "proname = 'lo_tell' OR "
                       "proname = 'loread' OR "
                       "proname = 'lowrite'")))
    (setq pg-lo-functions '())
    (mapc
     (lambda (tuple)
       (push (cons (car tuple) (cadr tuple)) pg-lo-functions))
     (pg-result res :tuples))
    (setq pg-lo-initialized t)))

;; fn is either an integer, in which case it is the OID of an element
;; in the pg_proc table, and otherwise it is a string which we look up
;; in the alist `pg-lo-functions' to find the corresponding OID.
(defun pg-fn (con fn integer-result &rest args)
  (pg-connection-set-busy con t)
  (unless pg-lo-initialized
    (pg-lo-init con))
  (let ((fnid (cond ((integerp fn) fn)
                    ((not (stringp fn))
                     (let ((msg (format "Expecting a string or an integer: %s" fn)))
                       (signal 'pg-protocol-error (list msg))))
                    ((assoc fn pg-lo-functions) ; blech
                     (cdr (assoc fn pg-lo-functions)))
                    (t
                     (error "Unknown builtin function %s" fn)))))
    (pg-send-char con ?F)
    (pg-send-char con 0)
    (pg-send-uint con fnid 4)
    (pg-send-uint con (length args) 4)
    (mapc (lambda (arg)
            (cond ((integerp arg)
                   (pg-send-uint con 4 4)
                   (pg-send-uint con arg 4))
                  ((stringp arg)
                   (pg-send-uint con (length arg) 4)
                   (pg-send con arg))
                  (t
                   (error "Unknown fastpath type %s" arg))))
          args)
    (pg-flush con)
    (cl-loop with result = (list)
          for c = (pg-read-char con) do
          (cl-case c
             ;; ErrorResponse
            (?E (pg-handle-error-response con "in pg-fn"))

            ;; FunctionResultResponse
            (?V (setq result t))

            ;; Nonempty response
            (?G
             (let* ((len (pg-read-net-int con 4))
                    (res (if integer-result
                             (pg-read-net-int con len)
                           (pg-read-chars con len))))
               (setq result res)))

            ;; NoticeResponse
            (?N
             (let ((notice (pg-read-string con pg--MAX_MESSAGE_LEN)))
               (message "NOTICE: %s" notice))
             (unix-sync))

            ;; ReadyForQuery message
            (?Z
             (let ((_msglen (pg-read-net-int con 4))
                   (status (pg-read-char con)))
               (when (eql ?E status)
                 (message "PostgreSQL ReadyForQuery message with error status"))
               (pg-connection-set-busy con nil)
               (cl-return-from pg-fn result)))

            ;; end of FunctionResult
            (?0 nil)

            (t (error "Unexpected character in pg-fn: %s" c))))))

;; returns an OID
(defun pg-lo-create (connection &optional args)
  (let* ((modestr (or args "r"))
         (mode (cond ((integerp modestr) modestr)
                     ((string= "r" modestr) pg--INV_READ)
                     ((string= "w" modestr) pg--INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg--INV_READ pg--INV_WRITE))
                     (t (error "pg-lo-create: bad mode %s" modestr))))
         (oid (pg-fn connection "lo_creat" t mode)))
    (cond ((not (integerp oid))
           (error "Returned value not an OID: %s" oid))
          ((zerop oid)
           (error "Can't create large object"))
          (t oid))))

;; args = modestring (default "r", or "w" or "rw")
;; returns a file descriptor for use in later pg-lo-* procedures
(defun pg-lo-open (connection oid &optional args)
  (let* ((modestr (or args "r"))
         (mode (cond ((integerp modestr) modestr)
                     ((string= "r" modestr) pg--INV_READ)
                     ((string= "w" modestr) pg--INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg--INV_READ pg--INV_WRITE))
                     (t (error "pg-lo-open: bad mode %s" modestr))))
         (fd (pg-fn connection "lo_open" t oid mode)))
    (unless (integerp fd)
      (error "Couldn't open large object"))
    fd))

(defsubst pg-lo-close (connection fd)
  (pg-fn connection "lo_close" t fd))

(defsubst pg-lo-read (connection fd bytes)
  (pg-fn connection "loread" nil fd bytes))

(defsubst pg-lo-write (connection fd buf)
  (pg-fn connection "lowrite" t fd buf))

(defsubst pg-lo-lseek (connection fd offset whence)
  (pg-fn connection "lo_lseek" t fd offset whence))

(defsubst pg-lo-tell (connection oid)
  (pg-fn connection "lo_tell" t oid))

(defsubst pg-lo-unlink (connection oid)
  (pg-fn connection "lo_unlink" t oid))

;; returns an OID
;; FIXME should use unwind-protect here
(defun pg-lo-import (connection filename)
  (let* ((buf (get-buffer-create (format " *pg-%s" filename)))
         (oid (pg-lo-create connection "rw"))
         (fdout (pg-lo-open connection oid "w"))
         (pos (point-min)))
    (with-current-buffer buf
      (insert-file-contents-literally filename)
      (while (< pos (point-max))
        (pg-lo-write
         connection fdout
         (buffer-substring-no-properties pos (min (point-max) (cl-incf pos 1024)))))
      (pg-lo-close connection fdout))
    (kill-buffer buf)
    oid))

(defun pg-lo-export (connection oid filename)
  (let* ((buf (get-buffer-create (format " *pg-%d" oid)))
         (fdin (pg-lo-open connection oid "r")))
    (with-current-buffer buf
      (cl-do ((str (pg-lo-read connection fdin 1024)
                   (pg-lo-read connection fdin 1024)))
          ((or (not str)
               (zerop (length str))))
        (insert str))
      (pg-lo-close connection fdin)
      (write-file filename))
    (kill-buffer buf)))



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
  "List of the databases in the PostgreSQL server to which we are connected via CON."
  (let ((res (pg-exec con "SELECT datname FROM pg_database")))
    (apply #'append (pg-result res :tuples))))

(defun pg-tables (con)
  "List of the tables present in the database we are connected to via CON.
Only tables to which the current user has access are listed."
  (cond ((> (pgcon-server-version-major con) 7)
         (let ((res (pg-exec con "SELECT table_name FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_type='BASE TABLE'")))
           (apply #'append (pg-result res :tuples))))
        (t
         (let ((res (pg-exec con "SELECT relname FROM pg_class c WHERE "
                             "c.relkind = 'r' AND "
                             "c.relname !~ '^pg_' AND "
                             "c.relname !~ '^sql_' ORDER BY relname")))
           (apply #'append (pg-result res :tuples))))))

(defun pg-columns (con table)
  "List of the columns present in TABLE over PostgreSQL connection CON."
  (cond ((> (pgcon-server-version-major con) 7)
         (let* ((sql (format "SELECT column_name FROM information_schema.columns
                       WHERE table_schema='public' AND table_name = '%s'" table))
                (res (pg-exec con sql)))
           (apply #'append (pg-result res :tuples))))
        (t
         (let* ((sql (format "SELECT * FROM %s WHERE 0 = 1" table))
                (res (pg-exec con sql)))
           (mapcar #'car (pg-result res :attributes))))))

(defun pg-column-default (con table column)
  "Return the default value for COLUMN in PostgreSQL TABLE.
Using connection to PostgreSQL CON."
  (let* ((sql "SELECT column_default FROM information_schema.columns
               WHERE (table_schema, table_name, column_name) = ('public', $1, $2)")
         (res (pg-exec-prepared con sql `((,table . "text") (,column . "text")))))
    (caar (pg-result res :tuples))))

(defun pg-backend-version (con)
  "Version and operating environment of backend that we are connected to by CON.
PostgreSQL returns the version as a string. CrateDB returns it as an integer."
  (let ((res (pg-exec con "SELECT version()")))
    (cl-first (pg-result res :tuple 0))))


;; support routines ============================================================

;; Called to handle a RowDescription message
;;
;; Attribute information is as follows
;;    attribute-name (string)
;;    attribute-type as an oid from table pg_type
;;    attribute-size (in bytes?)
(defun pg-read-attributes (con)
  (let* ((_msglen (pg-read-net-int con 4))
         (attribute-count (pg-read-net-int con 2))
         (attributes (list))
         (ce (pgcon-client-encoding con)))
    (cl-do ((i attribute-count (- i 1)))
        ((zerop i) (nreverse attributes))
      (let ((type-name  (pg-read-string con pg--MAX_MESSAGE_LEN))
            (_table-oid (pg-read-net-int con 4))
            (_col       (pg-read-net-int con 2))
            (type-oid   (pg-read-net-int con 4))
            (type-len   (pg-read-net-int con 2))
            (_type-mod  (pg-read-net-int con 4))
            (_format-code (pg-read-net-int con 2)))
        (push (list (pg-text-parser type-name ce) type-oid type-len) attributes)))))

;; a bitmap is a string, which we interpret as a sequence of bytes
(defun pg-bitmap-ref (bitmap ref)
  (let ((int (aref bitmap (floor ref 8))))
    (logand 128 (ash int (mod ref 8)))))

;; Read data following a DataRow message
(defun pg-read-tuple (con attributes)
  (let* ((num-attributes (length attributes))
         (col-count (pg-read-net-int con 2))
         (tuples (list))
         (ce (pgcon-client-encoding con)))
    (unless (eql col-count num-attributes)
      (signal 'pg-protocol-error '("Unexpected value for attribute count sent by backend")))
    (cl-do ((i 0 (+ i 1))
            (type-ids (mapcar #'cl-second attributes) (cdr type-ids)))
        ((= i num-attributes) (nreverse tuples))
      (let ((col-octets (pg-read-net-int con 4)))
        (cl-case col-octets
          (4294967295
           ;; this is "-1" (pg-read-net-int doesn't handle integer overflow), which indicates a
           ;; NULL column
           (push nil tuples))
          (0
           (push "" tuples))
          (t
           (let* ((col-value (pg-read-chars con col-octets))
                  (parsed (pg-parse col-value (car type-ids) ce)))
             (push parsed tuples))))))))

(defun pg-read-char (con)
  (let ((process (pgcon-process con)))
    ;; (accept-process-output process 0.1)
    (with-current-buffer (process-buffer process)
      (dotimes (_i (pgcon-timeout con))
        (when (null (char-after pgcon--position))
          (sleep-for 0.1)
          (accept-process-output process 1.0)))
      (when (null (char-after pgcon--position))
        (let ((msg (format "Timeout in pg-read-char reading from %s" con)))
          (signal 'pg-error (list msg))))
      (prog1 (char-after pgcon--position)
        (setq-local pgcon--position (1+ pgcon--position))))))

(defun pg-unread-char (con)
  (let ((process (pgcon-process con)))
    (with-current-buffer (process-buffer process)
      (setq-local pgcon--position (1- pgcon--position)))))

;; FIXME should be more careful here; the integer could overflow.
(defun pg-read-net-int (con bytes)
  (cl-do ((i bytes (- i 1))
          (accum 0))
      ((zerop i) accum)
    (setq accum (+ (* 256 accum) (pg-read-char con)))))

(defun pg-read-int (con bytes)
  (cl-do ((i bytes (- i 1))
          (multiplier 1 (* multiplier 256))
          (accum 0))
      ((zerop i) accum)
    (cl-incf accum (* multiplier (pg-read-char con)))))

(defun pg-read-chars-old (con howmany)
  (cl-do ((i 0 (+ i 1))
          (chars (make-string howmany ?.)))
      ((= i howmany) chars)
    (aset chars i (pg-read-char con))))

(defun pg-read-chars (con count)
  (let ((process (pgcon-process con)))
    (with-current-buffer (process-buffer process)
      (let* ((start pgcon--position)
             (end (+ start count)))
        ;; (accept-process-output process 0.1)
        (dotimes (_i (pgcon-timeout con))
          (when (> end (point-max))
            (sleep-for 0.1)
            (accept-process-output process 1.0)))
        (when (> end (point-max))
          (let ((msg (format "Timeout in pg-read-chars reading from %s" con)))
            (signal 'pg-error (list msg))))
        (prog1 (buffer-substring start end)
          (setq-local pgcon--position end))))))

;; read a null-terminated string
(defun pg-read-string (con maxbytes)
  (cl-loop for i below maxbytes
           for ch = (pg-read-char con)
           until (eql ch ?\0)
           concat (byte-to-string ch)))

(cl-defstruct pgerror
  severity sqlstate message detail hint table column dtype)

(defun pg-read-error-response (con)
  (let* ((response-len (pg-read-net-int con 4))
         (msglen (- response-len 4))
         (msg (pg-read-chars con msglen))
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
                  (?t
                   (setf (pgerror-table err) val))
                  (?c
                   (setf (pgerror-column err) val))
                  (?d
                   (setf (pgerror-dtype err) val))))
    err))

(defun pg-handle-error-response (con &optional context)
  "Handle an ErrorMessage from the backend we are connected to over CON.
Additional information CONTEXT can be optionally included in the error message
presented to the user."
  (let ((e (pg-read-error-response con))
        (extra (list)))
    (when (pgerror-detail e)
      (push ", " extra)
      (push (pgerror-detail e) extra))
    (when (pgerror-hint e)
      (push ", " extra)
      (push (format "hint: %s" (pgerror-hint e)) extra))
    (when (pgerror-table e)
      (push ", " extra)
      (push (format "table: %s" (pgerror-table e)) extra))
    (when (pgerror-column e)
      (push ", " extra)
      (push (format "column: %s" (pgerror-column e)) extra))
    (setf extra (nreverse extra))
    (pop extra)
    (setf extra (butlast extra))
    (when extra
      (setf extra (append (list " (") extra (list ")"))))
    ;; now read the ReadyForQuery message. We don't always receive this immediately; for example if
    ;; an incorrect username is sent during startup, PostgreSQL sends an ErrorMessage then an
    ;; AuthenticationSASL message. In that case, unread the message type octet so that it can
    ;; potentially be handled after the error is signaled.
    (let ((c (pg-read-char con)))
      (unless (eql c ?Z)
        (message "Unexpected message type after ErrorMsg: %s" c)
        (pg-unread-char con)))
    ;; message length then status, discarded
    (pg-read-net-int con 4)
    (pg-read-char con)
    (let ((msg (format "%s%s: %s%s"
                       (pgerror-severity e)
                       (or (concat " " context) "")
                       (pgerror-message e)
                       (apply #'concat extra))))
      (signal 'pg-error (list msg)))))

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
    (message "PostgreSQL %s %s %s"
             (pgerror-severity notice)
             (pgerror-message notice)
             (apply #'concat extra))))

;; higher order bits first / little endian
(defun pg-send-uint (con num bytes)
  (let ((process (pgcon-process con))
        (str (make-string bytes 0))
        (i (- bytes 1)))
    (while (>= i 0)
      (aset str i (% num 256))
      (setq num (floor num 256))
      (cl-decf i))
    (process-send-string process str)))

;; big endian
(defun pg-send-net-uint (con num bytes)
  (let ((process (pgcon-process con))
        (str (make-string bytes 0)))
    (dotimes (i bytes)
      (aset str i (% num 256))
      (setq num (floor num 256)))
    (process-send-string process str)))

(defun pg-send-char (con char)
  (let ((process (pgcon-process con)))
    (process-send-string process (char-to-string char))))

(defun pg-send-string (con str)
  (let ((process (pgcon-process con)))
    ;; the string with the null-terminator octet
    (process-send-string process (concat str (string 0)))))

(defun pg-send-octets (con octets)
  (let ((process (pgcon-process con)))
    (process-send-string process octets)))

(defun pg-send (con str &optional bytes)
  (let ((process (pgcon-process con))
        (padding (if (and (numberp bytes) (> bytes (length str)))
                     (make-string (- bytes (length str)) 0)
                   (make-string 0 0))))
    (process-send-string process (concat str padding))))


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
