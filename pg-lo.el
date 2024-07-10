;;; pg-lo.el --- Support for PostgreSQL large objects -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-2.0-or-later


;;; Commentary:

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
(declare-function pg-connection-set-busy "pg" (con busy))
(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-exec "pg" (con &rest args))


(defconst pg--INV_ARCHIVE 65536)         ; fe-lobj.c
(defconst pg--INV_WRITE   131072)
(defconst pg--INV_READ    262144)
(defconst pg--LO_BUFIZE   1024)
(defconst pg--MAX_MESSAGE_LEN    8192)   ; libpq-fe.h


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


(provide 'pg-lo)

;;; pg-lo.el ends here
