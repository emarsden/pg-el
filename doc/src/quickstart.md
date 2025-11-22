# Quickstart to using pg-el

These illustrative examples assume you have a PostgreSQL user `pgeltestuser` with password `pgeltest`
who owns a database `pgeltestdb`. To set that up with a local PostgreSQL database, use commands
similar to the following:

~~~admonish example title="Create a test user and database with a local PostgreSQL"
```shell
sudo -u postgres createuser --createdb pgeltestuser
sudo -u postgres createdb --owner=pgeltestuser pgeltestdb
sudo -u postgres psql
postgres=# alter user pgeltestuser with encrypted password 'pgeltest';
```

If you want to enable and test the support for the HSTORE and pgvector extensions, you will need to
load them into the test database as PostgreSQL superuser (the normal user `pgeltestuser` we created
above is not allowed to load extensions). The pgvector extension generally needs to be installed
separately from PostgreSQL (for example by installing the `postgresql-17-pgvector` package on Debian).

```shell
sudo -u postgres psql
postgres=# CREATE EXTENSION hstore;
CREATE EXTENSION
postgres=# CREATE EXTENSION vector;
CREATE EXTENSION
```

The same applies to the PostGIS extension.
~~~

Now, from your preferred Emacs Lisp shell (here `M-x ielm`), check that you are able to connect to
and authenticate with the database from Emacs:

~~~admonish example title="Connect to PostgreSQL from Emacs"
```lisp
ELISP> (require 'pg)
pg
ELISP> (defvar *pg* (pg-connect-plist "pgeltestdb" "pgeltestuser" :password "pgeltest" :host "localhost"))
*pg*
```
~~~


If you don’t already have PostgreSQL installed locally, it may be convenient for you to use
[PostgreSQL Docker Community images](https://hub.docker.com/_/postgres/), using
[Docker](https://www.docker.com/) or [Podman](https://podman.io/). I recommend installing Podman
because it’s fully free software, whereas Docker is partly commercial. Podman is also able to run
containers “rootless”, without special privileges, which is good for security, and doesn’t require a
background daemon. Podman has a docker-compatible commandline interface.

~~~admonish example title="Start up PostgreSQL inside a Podman container"
```shell
podman run -d --name pgsql \
   -v /dev/log:/dev/log \
   -v /var/run/postgresql:/var/run/postgresql \
   --publish 5432:5432 \
   -e POSTGRES_DB=pgeltestdb \
   -e POSTGRES_USER=pgeltestuser \
   -e POSTGRES_PASSWORD=pgeltest \
   docker.io/library/postgres:latest
```
~~~

then connect from Emacs with

    ELISP> (pg-connect-plist "pgeltestdb" "pgeltestuser" :password "pgeltest" :host "localhost")

or connect over a local Unix socket

    ELISP> (pg-connect-local "/var/run/postgresql/.s.PGSQL.5432" "pgeltestdb" "pgeltestuser "pgeltest")


Now some simple interactions with the database:

```lisp
ELISP> (pg-backend-version *pg*)
"PostgreSQL 18.1 (Debian 18.1-1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 15.2.0-8) 15.2.0, 64-bit"
ELISP> (let ((res (pg-exec *pg* "SELECT 42, 1.23")))
           (pg-result res :tuple 0))
(42 1.23)
```

Note that the first query has returned an Emacs Lisp string, and the second query has returned a
tuple (represented as a list) where the first value is an Emacs Lisp integer, and the second and
Emacs Lisp float. The pg-el library has ensured **automatic type coercion** from the SQL types to the
most appropriate Emacs Lisp type. 

The following example shows the output from a query that returns multiple rows. It returns a list of
tuples, each tuple containing a single integer.

~~~admonish example title="A query that returns multiple rows"
```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT * FROM generate_series(50,55)")))
          (pg-result res :tuples))
((50)
 (51)
 (52)
 (53)
 (54)
 (55))
```
~~~

An SQL query that returns no results will return the empty list.

```lisp
ELISP> (let ((res (pg-exec *pg* "SELECT 3 where 1=0")))
         (pg-result res :tuples))
nil
```


For more, see the [usage information](usage.html) and the [API documentation](API.html).
