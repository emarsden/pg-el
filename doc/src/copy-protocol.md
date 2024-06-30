# The COPY protocol

The [COPY protocol](https://www.postgresql.org/docs/current/sql-copy.html) can be used to send and
receive large amounts of data to/from PostgreSQL. It can be used with CSV or TSV data.

## From Emacs to PostgreSQL

The pg-el library allows you to COPY from an Emacs buffer into PostgreSQL using function
`pg-copy-from-buffer`, as illustrated below.


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

## From PostgreSQL to Emacs

You can copy from PostgreSQL into an Emacs buffer using the function `pg-copy-to-buffer`, as
illustrated below.

~~~admonish example title="Dumping a PostgreSQL table into an Emacs buffer as CSV"
```lisp
ELISP> (let ((res (pg-copy-to-buffer *pg* "COPY copy_csv TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
                                     (get-buffer-create "*pg-csv*"))))
          (pg-result res :status))
"COPY 1000"
```
~~~



The following more verbose example illustrates fetching CSV data from an online source, importing it
into PostgreSQL, removing some unneeded columns and querying the data.

~~~admonish example title="Fetching and querying online CSV datasets"
```lisp
ELISP> (with-temp-buffer
         (url-insert-file-contents "https://www.data.gouv.fr/fr/datasets/r/51606633-fb13-4820-b795-9a2a575a72f1")
         (pg-exec *pg* "CREATE TABLE cities(
              insee_code TEXT NOT NULL,
              city_code TEXT,
              zip_code NUMERIC,
              label TEXT NOT NULL,
              latitude FLOAT,
              longitude FLOAT,
              department_name TEXT,
              department_number VARCHAR(3),
              region_name TEXT,
              region_geojson_name TEXT)")
         (pg-result (pg-copy-from-buffer *pg* "COPY cities FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE)"
                                 (current-buffer)) :status))
"COPY 39145"
ELISP> (pg-result (pg-exec *pg* "ALTER TABLE cities DROP COLUMN region_name") :status)
"ALTER TABLE"
ELISP> (pg-result (pg-exec *pg* "ALTER TABLE cities DROP COLUMN region_geojson_name") :status)
"ALTER TABLE"
ELISP> (pg-result (pg-exec *pg* "ALTER TABLE cities DROP COLUMN label") :status)
"ALTER TABLE"
ELISP> (pg-result (pg-exec *pg* "SELECT * FROM cities WHERE city_code LIKE 'toulouse%'") :tuples)
(("39533" "toulouse le chateau" 39230 46.821901729 5.583200112 "jura" "39")
 ("31555" "toulouse" 31100 43.596037953 1.432094901 "haute-garonne" "31")
 ("31555" "toulouse" 31300 43.596037953 1.432094901 "haute-garonne" "31")
 ("31555" "toulouse" 31400 43.596037953 1.432094901 "haute-garonne" "31")
 ("31555" "toulouse" 31500 43.596037953 1.432094901 "haute-garonne" "31")
 ("31555" "toulouse" 31000 43.596037953 1.432094901 "haute-garonne" "31")
 ("31555" "toulouse" 31200 43.596037953 1.432094901 "haute-garonne" "31"))
```

~~~
