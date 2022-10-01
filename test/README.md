# Testing code for pg.el Emacs Lisp socket-level interface to PostgreSQL


## Testing with a local PostgreSQL implementation

To set up the tests, create a PostgreSQL user `pgeltestuser` who owns a database
`pgeltestdb`, for example with

    sudo -u postgres createuser --createdb pgeltestuser
    sudo -u postgres createdb --owner=pgeltestuser pgeltestdb
    sudo -u postgres psql
    postgres=# alter user pgeltestuser with encrypted password 'pgeltest';

Check that you are able to connect to and authenticate with the database from Emacs:

    ELISP> (pg-connect "pgeltestdb" "pgeltestuser" "pgeltest" "localhost" 5432)

Adjust the username and password as necessary in `with-pgtest-connection` then run the tests from
Emacs with

    ELISP> (pg-test)

Cleaning up:

    sudo -u postgres dropdb pgeltestdb
    sudo -u postgres dropuser pgeltestuser



## Testing with Docker/Podman images

It is convenient to test different PostgreSQL versions using the [PostgreSQL Docker Community
images](https://hub.docker.com/_/postgres/), using Docker or Podman. 

Example invocation: 

    sudo podman run -d --name pgsql \
       -v /dev/log:/dev/log \
       -v /var/run/postgresql:/var/run/postgresql \
       --publish 5432:5432 \
       -e POSTGRES_DB=pgeltestdb \
       -e POSTGRES_USER=pgeltestuser \
       -e POSTGRES_PASSWORD=pgeltest \
       docker.io/library/postgres:13

then from Emacs

    ELISP> (pg-connect "pgeltestdb" "pgeltestuser" "pgeltest" "localhost" 5432)

or to connect over a local Unix socket

    ELISP> (pg-connect-local "/var/run/postgresql/.s.PGSQL.5432" "pgeltestdb" "pgeltestuser "pgeltest")

Note that these Docker images don't include TLS support. If you want to run the Debian-based images
(it won't work with the Alpine-based ones) with a self-signed certificate, you can use

    sudo podman run -d --name pgsql \
       -v /dev/log:/dev/log \
       -v /var/run/postgresql:/var/run/postgresql \
       --publish 5432:5432 \
       -e POSTGRES_DB=pgeltestdb \
       -e POSTGRES_USER=pgeltestuser \
       -e POSTGRES_PASSWORD=pgeltest \
       docker.io/library/postgres:13 \
       -c ssl=on \
       -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
       -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key


## Testing the CockroachDB distributed database

[CockroachDB](https://github.com/cockroachdb/cockroach) is an open source distributed database
implemented in Golang, built on a strongly-consistent key-value store. It implements the PostgreSQL
wire protocol.

    sudo podman run --name cockroachdb \
       -v /dev/log:/dev/log \
       --publish 26257:26257 \
       -d cockroachdb/cockroach start-single-node --insecure
 
    ELISP> (pg-connect "postgres" "root" "" "localhost" 26257)

Note that CockroachDB does not have large object support. 



## Testing the CrateDB distributed database

[CrateDB](https://crate.io/) is an open source distributed database implemented in Java, that
implements the PostgreSQL wire protocol.

    sudo podman run --name cratedb \
       --publish 5432:5432 \
       docker.io/library/crate:latest -Cdiscovery.type=single-node
    # psql -h localhost -p 5432 -U crate
    crate=> CREATE USER pgeltestuser WITH (password = 'pgeltest');
    CREATE 1
    crate=> GRANT ALL PRIVILEGES TO pgeltestuser;
    GRANT 4
    ELISP> (pg-connect "postgres" "pgeltestuser" "pgeltest" "localhost" 5432)


We have a bug in our interaction with this database (as of CrateDB version 5.0.1) in that the result
of a `pg-exec` call returns the result from the previous command executed, rather than the command
just sent.



## Testing the QuestDB time series database

[QuestDB](https://questdb.io/) is an open source relational column-oriented database designed for
time series and event data. It implements the PostgreSQL wire protocol. 

    sudo podman run -p 8812:8812 questdb/questdb

    ELISP> (pg-connect "ignored" "admin" "quest" "localhost" 8812)



## Testing with older Emacs versions

    sudo podman run silex/emacs:26.3-alpine-ci
