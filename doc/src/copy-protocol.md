# The COPY protocol

The COPY protocol can be used to send large amounts of data to PostgreSQL. It can be used with CSV
or TSV data. The pg-el library allows you to COPY from an Emacs buffer into PostgreSQL.

~~~admonish example title="Inserting tab-separated data"
```lisp
ELISP> (defun ascii (n) (+ ?A (mod n 26)))
ascii
ELISP> (defun random-word () 
          (apply #'string (cl-loop for count to 10 collect (+ ?a (random 26)))))
random-word
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE copy_tsv(a INTEGER, b CHAR, c TEXT)") :status)
"CREATE TABLE"
ELISP> (let ((buf (get-buffer-create " *pg-copy-temp-tsv*")))
         (with-current-buffer buf
           (dotimes (i 42)
             (insert (format "%d\t%c\t%s\n" i (ascii i) (random-word)))))
         (pg-result (pg-copy-from-buffer *pg* "COPY copy_tsv(a,b,c) FROM STDIN" buf) :status))
"COPY 84"
ELISP> (pg-result (pg-exec *pg* "SELECT COUNT(*) FROM copy_tsv") :tuple 0)
(84)
ELISP> (pg-result (pg-exec *pg* "SELECT * FROM copy_tsv LIMIT 5") :tuples)
((0 "A" "ufyhdnkoyfi")
 (1 "B" "jpnlxbftdpm")
 (2 "C" "lqvazrhesdg")
 (3 "D" "epxkjdsfdpg")
 (4 "E" "yjhgdwjzbvt"))
```
~~~


~~~admonish example title="Inserting comma-separated data (CSV)"

The use of CSV formatted data is very similar; you simply need to specify `WITH (FORMAT CSV)` in the
[`COPY` statement](https://www.postgresql.org/docs/current/sql-copy.html).

```lisp
ELISP> (pg-result (pg-exec *pg* "CREATE TABLE copy_csv (a INT2, b INTEGER, c CHAR, d TEXT)") :status)
"CREATE TABLE"
ELISP> (let ((buf (get-buffer-create " *pg-copy-temp-csv*")))
         (with-current-buffer buf
           (dotimes (i 1000)
             (insert (format "%d,%d,%c,%s\n" i (* i i) (ascii i) (random-word)))))
         (pg-result (pg-copy-from-buffer *pg* "COPY copy_csv(a,b,c,d) FROM STDIN WITH (FORMAT CSV)" buf) :status))
"COPY 1000"
ELISP> (pg-result (pg-exec *pg* "SELECT * FROM copy_csv LIMIT 3") :tuples)
((0 0 "A" "ajoskqunbrx")
 (1 1 "B" "pzmoyefgywu")
 (2 4 "C" "blylbnhnrdb"))
```

~~~
