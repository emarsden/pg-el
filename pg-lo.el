;;; pg-lo.el --- Support for PostgreSQL large objects -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024-2025  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-3.0-or-later


;;; Commentary:

;; Humphrey: Who is Large and to what does he object?
;;
;; Large objects are the PostgreSQL way of doing what most databases call BLOBs
;; (binary large objects). In addition to being able to stream data to and from
;; large (up to 4TB in size) objects, PostgreSQL's object-relational
;; capabilities allow the user to provide functions which act on the objects.
;;
;; For example, the user can define a new type called "circle", and define a C
;; or Tcl function called `circumference' which will act on circles. There is
;; also an inheritance mechanism in PostgreSQL.
;;
;; https://www.postgresql.org/docs/current/lo-interfaces.html
;; https://www.postgresql.org/docs/current/lo-funcs.html

;;; Code:

(require 'cl-lib)
(require 'hex-util)

(declare-function pg--read-string "pg" (con maxbytes))
(declare-function pg--read-char "pg" (con))
(declare-function pg--read-chars "pg" (con count))
(declare-function pg--read-net-int "pg" (con bytes))
(declare-function pg-read-error-response "pg" (con))
(declare-function pg-handle-error-response "pg" (con &optional context))
(declare-function pg-flush "pg" (con))
(declare-function pg--send "pg" (con str &optional bytes))
(declare-function pg--send-uint "pg" (con num bytes))
(declare-function pg--send-char "pg" (con char))
(declare-function pg--send-octets "pg" (con octets))
(declare-function pg-connection-set-busy "pg" (con busy))
(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-exec "pg" (con &rest args))
(declare-function pg-exec-prepared "pg" (con query typed-arguments &rest args))
(declare-function pgcon-query-log "pg" (pgcon))
(declare-function pg--trim-connection-buffers "pg" (con))

;; Forward declaration to pacify the bytecode compiler. This variable is defined in pg.el.
(defvar pg-handle-notice-functions)

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
                       "proname = 'lo_truncate' OR "
                       "proname = 'loread' OR "
                       "proname = 'lowrite'")))
    (setq pg-lo-functions '())
    (mapc
     (lambda (tuple)
       (push (cons (car tuple) (cadr tuple)) pg-lo-functions))
     (pg-result res :tuples))
    (setq pg-lo-initialized t)))

;; Interface to PostgreSQL's "fast-path" interface, which makes it possible to
;; send simple function calls to the server. FN is either an integer, in which
;; case it is the OID of an element in the pg_proc table, and otherwise it is a
;; string which we look up in the alist `pg-lo-functions' to find the
;; corresponding OID.
(cl-defun pg-fn (con fn integer-result &rest args)
  (pg-connection-set-busy con t)
  (when (pgcon-query-log con)
    (let ((query (format "EXEC pg-fn %s %s %s" fn integer-result args)))
      (with-current-buffer (pgcon-query-log con)
        (insert query "\n"))
      (when noninteractive
        (message "%s" query))))
  (unless pg-lo-initialized
    (pg-lo-init con))
  (let ((fnid (cond ((integerp fn) fn)
                    ((not (stringp fn))
                     (let ((msg (format "Expecting a string or an integer: %s" fn)))
                       (signal 'pg-protocol-error (list msg))))
                    ((assoc fn pg-lo-functions) ; blech
                     (cdr (assoc fn pg-lo-functions)))
                    (t
                     (let ((msg (format "Unknown builtin function %s" fn)))
                       (signal 'pg-user-error (list msg)))))))
    ;; https://www.postgresql.org/docs/17/protocol-message-formats.html
    (pg--send-char con ?F)
    (let* ((arg-len (length args))
           (msg-len (+ 4 4 2 (* 2 arg-len) 2
                       (cl-loop for arg in args
                                sum (+ 4 (if (integerp arg) 4 (length arg))))
                       2)))
      (pg--send-uint con msg-len 4))
    (pg--send-uint con fnid 4)
    ; The number of argument format codes that follow
    (pg--send-uint con (length args) 2)
    ;; The argument format codes, either zero for text or 1 for binary.
    (dolist (arg args)
      (cond ((integerp arg)
             (pg--send-uint con 1 2))
            ((stringp arg)
             (pg--send-uint con 0 2))
            (t
             (let ((msg (format "Unknown fastpath type %s" arg)))
               (signal 'pg-user-error (list msg))))))
    ;; Send the number of arguments being specified to the function
    (pg--send-uint con (length args) 2)
    ;; Send length/value for each argument
    (dolist (arg args)
      (cond ((integerp arg)
             (pg--send-uint con 4 4)
             (pg--send-uint con arg 4))
            ((stringp arg)
             (pg--send-uint con (length arg) 4)
             (pg--send-octets con arg))))
    ;; Int16: the format code for the function result. Must presently be zero (text) or one (binary).
    (if integer-result
        (pg--send-uint con 1 2)
      (pg--send-uint con 0 2))
    (pg-flush con)
    (cl-loop with result = nil
             for c = (pg--read-char con) do
             (cl-case c
               ;; ErrorResponse
               (?E (pg-handle-error-response con "in pg-fn"))

               ;; FunctionCallResult
               (?V
                (let ((_msg-len (pg--read-net-int con 4))
                      (value-len (pg--read-net-int con 4)))
                  (setq result (if integer-result
                                   (pg--read-net-int con value-len)
                                 (pg--read-chars con value-len)))))

               ;; NoticeResponse
               (?N
                ;; a Notice response has the same structure and fields as an ErrorResponse
                (let ((notice (pg-read-error-response con)))
                  (dolist (handler pg-handle-notice-functions)
                    (funcall handler notice)))
                (when (fboundp 'unix-sync)
                  (unix-sync)))

               ;; ReadyForQuery message
               (?Z
                (let ((_msglen (pg--read-net-int con 4))
                      (status (pg--read-char con)))
                  (when (eql ?E status)
                    (message "PostgreSQL ReadyForQuery message with error status"))
                  (pg--trim-connection-buffers con)
                  (pg-connection-set-busy con nil)
                  (cl-return-from pg-fn result)))

               ;; end of FunctionResult
               (?0 nil)

               (t
                (let ((msg (format "Unexpected data from backend in pg-fn: %s" c)))
                  (signal 'pg-protocol-error (list msg))))))))

;; These constants are defined in libpq-fs.h in the PostgreSQL source.
(defconst pg--INV_WRITE   #x20000)
(defconst pg--INV_READ    #x40000)

;; Note: recent versions of PostgreSQL support an alternative function lo_create
;; which takes the desired oid as a parameter.
(defun pg-lo-create (con &optional mode)
  "Create a new large object using PostgreSQL connection CON.
Returns the OID of the new object. Note that the MODE argument is
ignored in PostgreSQL releases after v8.1."
  (let* ((modestr (or mode "r"))
         (mode (cond ((integerp modestr) modestr)
                     ((string= "r" modestr) pg--INV_READ)
                     ((string= "w" modestr) pg--INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg--INV_READ pg--INV_WRITE))
                     (t (let ((msg (format "pg-lo-create: bad mode %s" mode)))
                          (signal 'pg-user-error (list msg))))))
         (oid (pg-fn con "lo_creat" t mode)))
    (cond ((not (integerp oid))
           (let ((msg (format "Returned value not an OID: %s" oid)))
             (signal 'pg-operational-error (list msg))))
          ((eql -1 oid)
           (signal 'pg-operational-error (list "Can't create large object")))
          (t oid))))

(defun pg-lo-open (con oid &optional mode)
  "Open the PostgreSQL large object designated by OID for reading.
The string MODE determines whether the object is opened for
reading (`r'), or writing (`w'), or both (`rw'); it is ignored in
PostgreSQL releases after v8.1. Returns a large object descriptor that
can be used with functions `pg-lo-read', `pg-lo-write', `pg-lo-lseek',
`pg-lo-tell' and `pg-lo-close'. Uses PostgreSQL connection CON."
  (unless (> oid 0)
    (let ((msg (format "%s is not a valid PostgreSQL OID" oid)))
      (signal 'pg-user-error (list msg))))
  (let* ((modestr (or mode "r"))
         (mode (cond ((integerp modestr) modestr)
                     ((string= "r" modestr) pg--INV_READ)
                     ((string= "w" modestr) pg--INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg--INV_READ pg--INV_WRITE))
                     (t (let ((msg (format "pg-lo-open: bad mode %s" mode)))
                          (signal 'pg-user-error (list msg))))))
         (fd (pg-fn con "lo_open" t oid mode)))
    (if (eql -1 fd)
      (signal 'pg-operational-error (list "Couldn't open large object"))
    fd)))

(defun pg-lo-close (con fd)
  "Closes the PostgreSQL large object designated by FD.
Uses PostgreSQL connection CON."
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-close: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (let ((ret (pg-fn con "lo_close" t fd)))
    (if (eql -1 ret)
        (signal 'pg-operational-error (list "lo_close function call failed"))
      ret)))

(defun pg-lo-read (con fd bytes)
  "Read BYTES octets from PostgreSQL large object designated by FD.
Uses PostgreSQL connection CON."
  (unless (>= bytes 0)
    (signal 'pg-user-error (list "pg-lo-read: BYTES must be >= 0")))
  ;; This error will be caught and reported by the PostgreSQL backend, but instead of being detected
  ;; immediately when handling this function call request, it is detected after the next request is
  ;; made. It's more practical to detect it immediately on the client side.
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-read: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (let* ((encoded (pg-fn con "loread" nil fd bytes))
         (hexdigits (substring encoded 2)))
    (pg--trim-connection-buffers con)
    (unless (and (eql 92 (aref encoded 0))   ; \ character
                 (eql ?x (aref encoded 1)))
      (signal 'pg-protocol-error
              (list "Unexpected format for BYTEA binary string")))
    (decode-hex-string hexdigits)))

(defun pg-lo-write (con fd buf)
  "Write the contents of BUF to the large object designated by FD.
Uses PostgreSQL connection CON."
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-write: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (unless (stringp buf)
    (signal 'pg-user-error (list "pg-lo-write: invalid BUF argument")))
  (let ((ret (pg-fn con "lowrite" t fd buf)))
    (pg--trim-connection-buffers con)
    (if (eql -1 ret)
        (signal 'pg-operational-error (list "lo_write function call failed"))
      ret)))


(defconst pg-SEEK_SET 0)
(defconst pg-SEEK_CUR 1)
(defconst pg-SEEK_END 2)

(defun pg-lo-lseek (con fd offset whence)
  "Seek to position OFFSET in PostgreSQL large object designated by FD.
WHENCE can be `pg-SEEK_SET' (seek from object start),
`pg-SEEK_CUR' (seek from current position), or `pg-SEEK_END' (seek from
object end). OFFSET may be a large integer (int8 type in PostgreSQL;
this function calls the PostgreSQL backend function `lo_lseek64'). Uses
PostgreSQL connection CON."
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-lseek: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (let* ((res (pg-exec-prepared con "SELECT lo_lseek64($1, $2, $3)"
                                `((,fd . "int4") (,offset . "int8") (,whence . "int4"))))
         (ret (cl-first (pg-result res :tuple 0))))
    (if (eql -1 ret)
        (signal 'pg-operational-error (list "lo_lseek64 function call failed"))
      ret)))

(defun pg-lo-tell (con fd)
  "Return the current file position in PostgreSQL large object FD.
Uses PostgreSQL connection CON. Uses the PostgreSQL backend function
`lo_tell64' to work with large objects."
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-tell: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (let* ((res (pg-exec-prepared con "SELECT lo_tell64($1)" `((,fd . "int4"))))
         (ret (cl-first (pg-result res :tuple 0))))
    (if (eql -1 ret)
        (signal 'pg-operational-error (list "lo_tell64 function call failed"))
      ret)))

(defun pg-lo-truncate (con fd len)
  "Truncate the PostgreSQL large object FD to size LEN.
LEN may be a large integer (int8 type in PostgreSQL); this calls the
PostgreSQL backend function `lo_truncate64'. Uses PostgreSQL connection
CON."
  (unless (>= fd 0)
    (let ((msg (format "pg-lo-truncate: invalid FD %s" fd)))
      (signal 'pg-user-error (list msg))))
  (let* ((res (pg-exec-prepared con "SELECT lo_truncate64($1, $2)"
                                `((,fd . "int4") (,len . "int8"))))
         (ret (cl-first (pg-result res :tuple 0))))
    (unless (zerop ret)
      (signal 'pg-operational-error (list "lo_truncate64 function call failed"))))) 

(defun pg-lo-unlink (con oid)
  "Unlink (delete) the PostgreSQL large object identified by OID.
Uses PostgreSQL connection CON."
  (unless (> oid 0)
    (let ((msg (format "%s is not a valid PostgreSQL OID" oid)))
      (signal 'pg-user-error (list msg))))
  (let ((ret (pg-fn con "lo_unlink" t oid)))
    (if (eql -1 ret)
        (signal 'pg-operational-error (list "lo_unlink function call failed"))
      ret)))

;; FIXME should use unwind-protect here
(defun pg-lo-import (con filename)
  "Import FILENAME as a PostgreSQL large object.
Returns the OID of the new object. Note that FILENAME is a client-side
file path, rather than a server-side path as used by the `lo_import'
function in libpq. Uses PostgreSQL connection CON."
  (let* ((buf (get-buffer-create (format " *pg-%s" filename)))
         (oid (pg-lo-create con "rw"))
         (fdout (pg-lo-open con oid "w"))
         (pos nil))
    (with-current-buffer buf
      (insert-file-contents-literally filename)
      (setq pos (point-min))
      (while (< pos (point-max))
        (pg-lo-write con fdout
         (buffer-substring-no-properties pos (min (point-max) (cl-incf pos 1024)))))
      (pg-lo-close con fdout))
    (kill-buffer buf)
    oid))

;; Note that we are not using the lo_export fastpath function, in order to use a client-side output
;; path, rather than an output path on the server where PostgreSQL is running.
(defun pg-lo-export (con oid filename)
  "Export PostgreSQL large object identified by OID to FILENAME.
Note that FILENAME is a client-side file path, rather than a server-side
path as used by the `lo_export' function in libpq. Uses PostgreSQL
connection CON."
  (let* ((buf (get-buffer-create (format " *pg-%d" oid)))
         (fdin (pg-lo-open con oid "r")))
    (with-current-buffer buf
      (cl-do ((str (pg-lo-read con fdin 1024)
                   (pg-lo-read con fdin 1024)))
          ((or (not str)
               (zerop (length str))))
        (insert str))
      (pg-lo-close con fdin)
      (write-file filename))
    (kill-buffer buf)))


(provide 'pg-lo)

;;; pg-lo.el ends here
