# The HSTORE key-value type

There is support for the PostgreSQL [HSTORE
  extension](https://www.postgresql.org/docs/current/hstore.html), which can store key/value pairs
  in a single PostgreSQL column. It's necessary to call `pg-hstore-setup` before using this
  functionality, to load the extension if necessary and to set up our parser support for the HSTORE
  type.

~~~admonish example title="Using HSTORE values"
```lisp
ELISP> (pg-hstore-setup *pg*)
ELISP> (defvar *hs* (car (pg-result (pg-exec *pg* "SELECT 'foo=>bar'::hstore") :tuple 0)))
*hs*
ELISP> (gethash "foo" *hs*)
"bar"
ELISP> (hash-table-count *hs*)
1 (#o1, #x1, ?\C-a)
;; There is no guarantee as to the value stored for the 'a' key (duplicate)
ELISP> (setq *hs* (car (pg-result (pg-exec *pg* "SELECT 'a=>1,foobles=>2,a=>66'::hstore") :tuple 0)))
#<hash-table equal 2/65 0x1574257479d9>
ELISP> (hash-table-count *hs*)
2 (#o2, #x2, ?\C-b)
ELISP> (pg-result (pg-exec *pg* "SELECT akeys('biz=>NULL,baz=>42,boz=>66'::hstore)") :tuple 0)
(["baz" "biz" "boz"])
```

~~~



~~~admonish example title="Serialization support for HSTORE values"
```lisp
ELISP> (pg-hstore-setup *pg*)
ELISP> (let ((ht (make-hash-table :test #'equal)))
        (puthash "biz" "baz" ht)
        (puthash "foo" "bar" ht)
        (puthash "more" "than" ht)
        (let* ((res (pg-exec-prepared con "SELECT $1 ? 'foo'" `((,ht . "hstore"))))
            (pg-result res :tuple 0))))
(t)
```

~~~
