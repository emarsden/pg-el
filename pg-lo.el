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


;;; Code:

(require 'cl-lib)

(declare-function pg-read-string "pg" (con maxbytes))
(declare-function pg-read-char "pg" (con))
(declare-function pg-read-chars "pg" (con count))
(declare-function pg-read-net-int "pg" (con bytes))
(declare-function pg-handle-error-response "pg" (con &optional context))
(declare-function pg-flush "pg" (con))
(declare-function pg-send "pg" (con str &optional bytes))
(declare-function pg-send-uint "pg" (con num bytes))
(declare-function pg-send-char "pg" (con char))
(declare-function pg-send-octets "pg" (con octets))
(declare-function pg-connection-set-busy "pg" (con busy))
(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-exec "pg" (con &rest args))



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

;; Interface to PostgreSQL's "fast-path" interface, which makes it possible to
;; send simple function calls to the server. FN is either an integer, in which
;; case it is the OID of an element in the pg_proc table, and otherwise it is a
;; string which we look up in the alist `pg-lo-functions' to find the
;; corresponding OID.
(cl-defun pg-fn (con fn integer-result &rest args)
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
                     (let ((msg (format "Unknown builtin function %s" fn)))
                       (signal 'pg-user-error (list msg)))))))
    ;; https://www.postgresql.org/docs/17/protocol-message-formats.html
    (pg-send-char con ?F)
    (let* ((arg-len (length args))
           (msg-len (+ 4 4 2 (* 2 arg-len) 2
                       (cl-loop for arg in args
                                sum (+ 4 (if (integerp arg) 4 (length arg))))
                       2)))
      (pg-send-uint con msg-len 4))
    (pg-send-uint con fnid 4)
    ; The number of argument format codes that follow
    (pg-send-uint con (length args) 2)
    ;; The argument format codes, either zero for text or 1 for binary.
    (dolist (arg args)
      (cond ((integerp arg)
             (pg-send-uint con 1 2))
            ((stringp arg)
             (pg-send-uint con 0 2))
            (t
             (let ((msg (format "Unknown fastpath type %s" arg)))
               (signal 'pg-user-error (list msg))))))
    ;; Send the number of arguments being specified to the function
    (pg-send-uint con (length args) 2)
    ;; Send length/value for each argument
    (dolist (arg args)
      (cond ((integerp arg)
             (pg-send-uint con 4 4)
             (pg-send-uint con arg 4))
            ((stringp arg)
             (pg-send-uint con (length arg) 4)
             (pg-send-octets con arg))))
    ;; Int16: the format code for the function result. Must presently be zero (text) or one (binary).
    (if integer-result
        (pg-send-uint con 1 2)
      (pg-send-uint con 0 2))
    (pg-flush con)
    (cl-loop with result = nil
             for c = (pg-read-char con) do
             (cl-case c
               ;; ErrorResponse
               (?E (pg-handle-error-response con "in pg-fn"))

               ;; FunctionCallResult
               (?V
                (let ((msg-len (pg-read-net-int con 4))
                      (value-len (pg-read-net-int con 4)))
                  (setq result (if integer-result
                                   (pg-read-net-int con value-len)
                                 (pg-read-chars con value-len)))))

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
                (let ((_msglen (pg-read-net-int con 4))
                      (status (pg-read-char con)))
                  (when (eql ?E status)
                    (message "PostgreSQL ReadyForQuery message with error status"))
                  (pg-connection-set-busy con nil)
                  (cl-return-from pg-fn result)))

               ;; end of FunctionResult
               (?0 nil)

               (t (error "Unexpected character in pg-fn: %s" c))))))

(defconst pg--INV_WRITE   131072)       ; fe-lobj.c
(defconst pg--INV_READ    262144)

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
           (error "Returned value not an OID: %s" oid))
          ((zerop oid)
           (error "Can't create large object"))
          (t oid))))

;; mode = modestring (default "r", or "w" or "rw")
(defun pg-lo-open (connection oid &optional mode)
  "Open the PostgreSQL large object designated by OID for reading.
Uses PostgreSQL connection CON. The string MODE determines whether the
object is opened for reading (`r'), or writing (`w'), or both (`rw').
Returns a large object descriptor that can be used with functions
`pg-lo-read', `pg-lo-write', `pg-lo-lseek', `pg-lo-tell' and `pg-lo-close'."
  (let* ((modestr (or mode "r"))
         (mode (cond ((integerp modestr) modestr)
                     ((string= "r" modestr) pg--INV_READ)
                     ((string= "w" modestr) pg--INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg--INV_READ pg--INV_WRITE))
                     (t (let ((msg (format "pg-lo-open: bad mode %s" mode)))
                          (signal 'pg-user-error (list msg))))))
         (fd (pg-fn connection "lo_open" t oid mode)))
    (unless (integerp fd)
      (error "Couldn't open large object"))
    fd))

;; TODO: should we be checking the return value and signalling a pg-error on failure?
(defsubst pg-lo-close (connection fd)
  (pg-fn connection "lo_close" t fd))

(defun pg-lo-read (connection fd bytes)
  (let* ((encoded (pg-fn connection "loread" nil fd bytes))
         (hexdigits (substring encoded 2)))
    ;; (message "lo-read: hex encoded is %s" encoded)
    (unless (and (eql 92 (aref encoded 0))   ; \ character
                 (eql ?x (aref encoded 1)))
      (signal 'pg-protocol-error
              (list "Unexpected format for BYTEA binary string")))
    (decode-hex-string hexdigits)))

(defsubst pg-lo-write (connection fd buf)
  (pg-fn connection "lowrite" t fd buf))


(defconst pg--SEEK_SET 0)
(defconst pg--SEEK_CUR 1)
(defconst pg--SEEK_END 2)


;; Whence can be SEEK_SET (seek from object start), SEEK_CUR (seek from current
;; position), and SEEK_END (seek from object end).
(defsubst pg-lo-lseek (connection fd offset whence)
  (pg-fn connection "lo_lseek" t fd offset whence))

(defsubst pg-lo-tell (connection oid)
  (pg-fn connection "lo_tell" t oid))

(defsubst pg-lo-truncate (con oid len)
  (pg-fn con "lo_truncate" oid len))

(defsubst pg-lo-unlink (connection oid)
  (pg-fn connection "lo_unlink" t oid))

;; FIXME should use unwind-protect here
(defun pg-lo-import (con filename)
  "Import FILENAME as a PostgreSQL large object.
Uses PostgreSQL connection CON. Returns the OID of the new object."
  (let* ((buf (get-buffer-create (format " *pg-%s" filename)))
         (oid (pg-lo-create con "rw"))
         (fdout (pg-lo-open con oid "w"))
         (pos (point-min)))
    (with-current-buffer buf
      (insert-file-contents-literally filename)
      (while (< pos (point-max))
        (pg-lo-write con fdout
         (buffer-substring-no-properties pos (min (point-max) (cl-incf pos 1024)))))
      (pg-lo-close con fdout))
    (kill-buffer buf)
    oid))

(defun pg-lo-export (con oid filename)
  "Export PostgreSQL large object desingated by OID to FILENAME.
Uses PostgreSQL connection CON."
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
