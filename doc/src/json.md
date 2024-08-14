# JSON and JSONB values

PostgreSQL has quite a lot of [support for storing, saving and processing JSON and JSONB
data](https://www.postgresql.org/docs/current/functions-json.html). pg-el is able to deserialize
JSON and JSONB values into Emacs Lisp structures such as hashtables (for dicts), arrays, numbers,
strings and so on.

This library will parse and represent JSON/JSONB data either using the JSON support built into Emacs
with libjansson (see function `json-available-p`, from version 28.1), or using the `json.el`
library. There are some differences in the ways these methods handle dictionaries and specific
values such as NULL, false, [] and {}. Our examples below use the builtin JSON support in Emacs.


~~~admonish example title="Retrieving and manipulating JSON data"
```lisp
ELISP> (defun scalar (sql) (car (pg-result (pg-exec *pg* sql) :tuple 0)))
scalar
ELISP> (let ((json (scalar "SELECT '[5,7]'::json")))
         (aref json 0))
5 (#o5, #x5, ?\C-e)
ELISP> (let ((json (scalar "SELECT '[42.0,77.7]'::jsonb")))
         (aref json 1))
77.7
ELISP> (scalar "SELECT '[]'::json")
[]
ELISP> (scalar "SELECT '{}'::json")
#<hash-table equal 0/1 0x1586e6cc2813>
ELISP> (let ((json (scalar "SELECT '{\"a\": 42, \"b\": \"foo\"}'::json")))
         (gethash "b" json))
"foo"
ELISP> (let ((json (scalar "SELECT '{\"a\": [0,1,2,null]}'::json")))
         (gethash "a" json))
[0 1 2 :null]
```
~~~


pg-el can also serialize Emacs Lisp structures into the PostgreSQL JSON format, for use in prepared
statements.

~~~admonish example title="Serializing objects to JSON / JSONB"
```lisp
ELISP> (let ((ht (make-hash-table)))
         (puthash "biz" 45 ht)
         (puthash "boz" -5.5 ht)
         (puthash "comment" "good stuff" ht)
         (pg-result (pg-exec-prepared *pg* "SELECT $1->'boz'" `((,ht . "json"))) :tuple 0))
(-5.5)
ELISP> (let ((ht (make-hash-table)))
         (puthash "biz" 45 ht)
         (puthash "boz" -5.5 ht)
         (puthash "comment" "good stuff" ht)
         ;; the '-' jsonb operator deletes a matching key/value mapping
         (let* ((res (pg-exec-prepared *pg* "SELECT $1 - 'boz'" `((,ht . "jsonb"))))
                (row (pg-result res :tuple 0)))
           (gethash "comment" (cl-first row) )))
"good stuff"
```
~~~



## Support for the JSON path language (jsonpath type)

pg-el serializes and deserializes [JSONPATH
expressions](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH) as
strings, as illustrated below. You can use them as arguments to prepared statements.


~~~admonish example title="Serializing and deserializing JSON path expressions"
```lisp
ELISP> (pg-result (pg-exec *pg* "SELECT 'true'::jsonpath") :tuple 0)
(list "true")
ELISP> (pg-result (pg-exec *pg* "SELECT '$[*] ? (@ < 1 || @ > 5)'::jsonpath") :tuple 0)
(list "$[*]?(@ < 1 || @ > 5)")
ELISP> (let* ((sql "SELECT jsonb_path_query($1, $2)")
              (dict (make-hash-table :test #'equal))
              (_ (puthash "h" 5.6 dict))
              (params `((,dict . "jsonb") ("$.h.floor()" . "jsonpath")))
              (res (pg-exec-prepared con sql params))
              (row (pg-result res :tuple 0)))
          (cl-first row))
5
```
~~~

