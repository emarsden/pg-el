;;; Tests for the pg.el library
;;;
;;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;; Copyright: (C) 2022  Eric Marsden


(require 'pg)

;; For CockroachDB with default settings 
(defmacro with-pgtest-connection (conn &rest body)
  `(with-pg-connection ,conn ("postgres" "root" "" "localhost" 26257)
      ,@body))

;; For CrateDB
(defmacro with-pgtest-connection (conn &rest body)
  `(with-pg-connection ,conn ("doc" "pgeltestuser" "pgeltest")
                       ,@body))

;; For PostgreSQL, including for GitHub CI "test" workflow
(defmacro with-pgtest-connection (conn &rest body)
  (let ((db (or (getenv "PGEL_DATABASE") "pgeltestdb"))
        (user (or (getenv "PGEL_USER") "pgeltestuser"))
        (password (or (getenv "PGEL_PASSWORD") "pgeltest"))
        (host (or (getenv "PGEL_HOSTNAME") "localhost"))
        (port (or (getenv "PGEL_PORT") 5432)))
    `(with-pg-connection ,conn (,db ,user ,password ,host ,port)
         ,@body)))

;; Connect to the database over an encrypted (TLS) connection
(defmacro with-pgtest-connection-tls (conn &rest body)
  `(with-pg-connection ,conn ("pgeltestdb" "pgeltestuser" "pgeltest" "localhost" 5432 t)
                       ,@body))

(defun pg-test ()
  (with-pgtest-connection conn
   (message "Running pg.el tests against backend %s"
            (pg-backend-version conn))
   (let ((databases (pg-databases conn)))
     (if (member "pgeltest" databases)
         (pg-exec conn "DROP DATABASE pgeltest"))
     (pg-exec conn "CREATE DATABASE pgeltest"))
   (message "Testing insertions...")
   (pg-test-insert)
   (message "Testing date routines...")
   (pg-test-date)
   (message "Testing field extraction routines...")
   (pg-test-result)
   ;; (message "Testing large-object routines...")
   ;; (pg-test-lo-read)
   ;; (pg-test-lo-import)
   (pg-exec conn "DROP DATABASE pgeltest")
   (message "Tests passed")))

(defun pg-test-tls ()
  (with-pgtest-connection-tls conn
    (message "Running pg.el tests over TLS against backend %s"
             (pg-backend-version conn))
    (let ((databases (pg-databases conn)))
      (if (member "pgeltest" databases)
          (pg-exec conn "DROP DATABASE pgeltest"))
      (pg-exec conn "CREATE DATABASE pgeltest"))
    (message "Testing insertions...")
    (pg-test-insert)
    (message "Testing date routines...")
    (pg-test-date)
    (message "Testing field extraction routines...")
    (pg-test-result)
    (pg-exec conn "DROP DATABASE pgeltest")
    (message "Tests passed")))


(defun pg-test-insert (&optional count)
  (with-pgtest-connection conn
   (let ((res (list))
         (count (or count 100)))
     (pg-exec conn "CREATE TABLE count_test(key int, val int)")
     (cl-loop for i from 1 to count
           for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
                             i (* i i))
           do (pg-exec conn sql))
     (setq res (pg-exec conn "SELECT count(val) FROM count_test"))
     (cl-assert (= count (cl-first (pg-result res :tuple 0))))
     (setq res (pg-exec conn "SELECT sum(key) FROM count_test"))
     (cl-assert (= (cl-first (pg-result res :tuple 0))
                   (/ (* count (1+ count)) 2)))
     (pg-exec conn "DROP TABLE count_test"))))

;; Testing for the time handling routines. Expected output is
;; something like (in buffer *Messages*)
;;
;; timestamp = (14189 17420)
;; abstime = (14189 17420)
;; time = 19:42:06
(defun pg-test-date ()
  (with-pgtest-connection conn
   (let (res)
     (pg-exec conn "CREATE TABLE date_test(a timestamp, b time)")
     (pg-exec conn "INSERT INTO date_test VALUES "
              "(current_timestamp, 'now')")
     (setq res (pg-exec conn "SELECT * FROM date_test"))
     (setq res (pg-result res :tuple 0))
     (message "timestamp = %s" (cl-first res))
     (message "time = %s" (cl-second res)))
   (pg-exec conn "DROP TABLE date_test")))

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
(defun pg-test-result ()
  (with-pgtest-connection conn
   (let ((r1 (pg-exec conn "CREATE TABLE resulttest (a int, b VARCHAR(4))"))
         (r2 (pg-exec conn "INSERT INTO resulttest VALUES (3, 'zae')"))
         (r3 (pg-exec conn "INSERT INTO resulttest VALUES (66, 'poiu')"))
         (r4 (pg-exec conn "SELECT * FROM resulttest"))
         (r5 (pg-exec conn "DROP TABLE resulttest"))
         (r6 (pg-exec conn "SELECT generate_series(1, 10)")))
     (message "==============================================")
     (message "status of CREATE is %s" (pg-result r1 :status))
     (message "status of INSERT is %s" (pg-result r2 :status))
     (message "oid of INSERT is %s"    (pg-result r2 :oid))
     (message "status of SELECT is %s" (pg-result r4 :status))
     (message "attributes of SELECT are %s" (pg-result r4 :attributes))
     (message "tuples of SELECT are %s" (pg-result r4 :tuples))
     (message "second tuple of SELECT is %s" (pg-result r4 :tuple 1))
     (message "status of DROP is %s" (pg-result r5 :status))
     (message "==============================================")
     (cl-assert (eql (length (pg-result r6 :tuples)) 10)))))

;; test of large-object interface. Note the use of with-pg-transaction
;; to wrap the requests in a BEGIN..END transaction which is necessary
;; when working with large objects.
(defun pg-test-lo-read ()
  (with-pgtest-connection conn
   (with-pg-transaction conn
    (let* ((oid (pg-lo-create conn "rw"))
           (fd (pg-lo-open conn oid "rw")))
      (message "==================================================")
      (pg-lo-write conn fd "Hi there mate")
      (pg-lo-lseek conn fd 3 0)           ; SEEK_SET = 0
      (unless (= 3 (pg-lo-tell conn fd))
        (error "lo-tell test failed!"))
      (message "Read %s from lo" (pg-lo-read conn fd 7))
      (message "==================================================")
      (pg-lo-close conn fd)
      (pg-lo-unlink conn oid)))))

(defun pg-test-lo-import ()
  (with-pgtest-connection conn
   (with-pg-transaction conn
    (let ((oid (pg-lo-import conn "/etc/group")))
      (pg-lo-export conn oid "/tmp/group")
      (cond ((zerop (call-process "diff" nil nil nil "/tmp/group" "/etc/group"))
             (message "lo-import test succeeded")
             (delete-file "/tmp/group"))
            (t
             (message "lo-import test failed: check differences")
             (message "between files /etc/group and /tmp/group")))
      (pg-lo-unlink conn oid)))))

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


;; EOF
