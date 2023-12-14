# The COPY protocol

The COPY protocol can be used to send large amounts of data to PostgreSQL. It can be used with CSV
or TSV data. The pg-el library allows you to COPY from an Emacs buffer into PostgreSQL.

```lisp
(cl-flet ((ascii (n) (+ ?A (mod n 26)))
            (random-word () (apply #'string (cl-loop for count to 10 collect (+ ?a (random 26))))))
    (pg-exec conn "DROP TABLE IF EXISTS copy_tsv")
    (pg-exec conn "CREATE TABLE copy_tsv (a INTEGER, b CHAR, c TEXT)")
    (let ((buf (get-buffer-create " *pg-copy-temp-tsv*")))
      (with-current-buffer buf
        (dotimes (i 42)
          (insert (format "%d\t%c\t%s\n" i (ascii i) (random-word)))))
      (pg-copy-from-buffer conn "COPY copy_tsv(a,b,c) FROM STDIN" buf)
      (let ((res (pg-exec conn "SELECT count(*) FROM copy_tsv")))
        (should (eql 42 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec conn "SELECT sum(a) FROM copy_tsv")))
        (should (eql 861 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec conn "SELECT * FROM copy_tsv LIMIT 5")))
        (message "COPYTSV> %s" (pg-result res :tuples))))
    (pg-exec conn "DROP TABLE copy_tsv")
    (pg-exec conn "DROP TABLE IF EXISTS copy_csv")
    (pg-exec conn "CREATE TABLE copy_csv (a INT2, b INTEGER, c CHAR, d TEXT)")
    (let ((buf (get-buffer-create " *pg-copy-temp-csv*")))
      (with-current-buffer buf
        (dotimes (i 1000)
          (insert (format "%d,%d,%c,'%s'\n" i (* i i) (ascii i) (random-word)))))
      (pg-copy-from-buffer conn "COPY copy_csv(a,b,c,d) FROM STDIN WITH (FORMAT CSV)" buf)
      (let ((res (pg-exec conn "SELECT count(*) FROM copy_csv")))
        (should (eql 1000 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec conn "SELECT max(b) FROM copy_csv")))
        (should (eql 998001 (car (pg-result res :tuple 0)))))
      (let ((res (pg-exec conn "SELECT * FROM copy_csv LIMIT 3")))
        (message "COPYCSV> %s" (pg-result res :tuples)))
      (pg-exec conn "DROP TABLE copy_csv"))))
```

