# Collation

Case support in PostgreSQL (`lower()` and `upper()` functions) depend on the current [collation
rules](https://www.postgresql.org/docs/current/collation.html). A table has a default collation
which is specified at creation (with a default). To remove the dependency on the
table's collation, you can specify the desired collation explicitly.

Note that PostgreSQL can be compiled with or without support for libicu, as a complement to the
collation support in your libc.


> [!NOTE] 
>
> **Using different collation rules**
>
> ```lisp
> ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔÖÉ' COLLATE \"fr_FR\")")))
>          (car (pg-result res :tuple 0)))
> "fôöé"
> ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔ🐘💥bz' COLLATE \"fr_FR\")")))
>          (car (pg-result res :tuple 0)))
> "fô🐘💥bz"
> ELISP> (pg-result (pg-exec *pg* "CREATE COLLATION IF NOT EXISTS \"french\" (provider = icu, locale = 'fr_FR')") :status)
> "CREATE COLLATION"
> ELISP> (let ((res (pg-exec *pg* "SELECT lower('FÔÖÉ' COLLATE \"french\")")))
>          (car (pg-result res :tuple 0)))
> "fôöé"
> ```


