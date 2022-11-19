# Testing code for pg.el Emacs Lisp socket-level interface to PostgreSQL


This file contains some information on how to test the pg.el library against a locally accessible
PostgreSQL server, or running in a Docker/Podman container. It also shows how to test with other
databases that are compatible with the PostgreSQL wire protocol, including CockroachDB, CrateDB and
QuestDB. It also shows how to test with old Emacs versions running in a Docker/Podman container.

Some of these tests are set up in the GitHub Actions continuous integration service of our
repository (see YAML files in the `.github/workflows` directory).


## Testing with a local PostgreSQL implementation

To set up the tests, create a PostgreSQL user `pgeltestuser` who owns a database
`pgeltestdb`, for example with

    sudo -u postgres createuser --createdb pgeltestuser
    sudo -u postgres createdb --owner=pgeltestuser pgeltestdb
    sudo -u postgres psql
    postgres=# alter user pgeltestuser with encrypted password 'pgeltest';

Check that you are able to connect to and authenticate with the database from Emacs:

    ELISP> (defvar *pg* (pg-connect "pgeltestdb" "pgeltestuser" "pgeltest" "localhost" 5432))

Adjust the username and password as necessary in `with-pgtest-connection` then run the tests from
Emacs with

    ELISP> (pg-test)
    ELISP> (pg-test-tls)
    ELISP> (pg-test-local)

to test over respectively a standard TCP connection, a TCP connection with TLS encryption, and a
local Unix socket (on platforms on which this is supported).

You can also run `make test` from the `test` directory to run these tests from the commandline (as
well as `make test-tls`, `make test-local`).

Cleaning up after running the tests:

    sudo -u postgres dropdb pgeltestdb
    sudo -u postgres dropuser pgeltestuser



## Testing with Docker/Podman images

It is convenient to test different PostgreSQL versions using the [PostgreSQL Docker Community
images](https://hub.docker.com/_/postgres/), using Docker or Podman. 

Example invocation: 

    podman run -d --name pgsql \
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

    podman run -d --name pgsql \
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

    podman run --name cockroachdb \
       -v /dev/log:/dev/log \
       --publish 26257:26257 \
       -d cockroachdb/cockroach start-single-node --insecure

    ELISP> (pg-connect "postgres" "root" "" "localhost" 26257)
    
    PGEL_DATABASE=postgres PGEL_USER=root PGEL_PASSWORD="" PGEL_PORT=26257 make test

    podman stop cockroachdb
    podman rm cockroachdb

Note that CockroachDB does not have large object support. 



## Testing the CrateDB distributed database

[CrateDB](https://crate.io/) is an open source distributed database implemented in Java, that
implements the PostgreSQL wire protocol.

    podman run --name cratedb \
       --publish 5432:5432 \
       -d docker.io/library/crate:latest -Cdiscovery.type=single-node
    # psql -h localhost -p 5432 -U crate
    crate=> CREATE USER pgeltestuser WITH (password = 'pgeltest');
    CREATE 1
    crate=> GRANT ALL PRIVILEGES TO pgeltestuser;
    GRANT 4

    ELISP> (pg-connect "postgres" "pgeltestuser" "pgeltest" "localhost" 5432)

    PGEL_DATABASE=postgres PGEL_USER=pgeltestuser PGEL_PASSWORD="pgeltest" PGEL_PORT=5432 make test

    podman stop cratedb
    podman rm cratedb


Note that CrateDB doesn't implement COPY or large object support, nor PostgreSQL's full-text search
operators (it has specific features for text search). 


## Testing the QuestDB time series database

[QuestDB](https://questdb.io/) is an open source relational column-oriented database designed for
time series and event data. It implements the PostgreSQL wire protocol. 

    podman run --name questdb \
       --publish 8812:8812 \
       -d questdb/questdb

    ELISP> (pg-connect "ignored" "admin" "quest" "localhost" 8812)

    PGEL_DATABASE=postgres PGEL_USER=admin PGEL_PASSWORD="quest" PGEL_PORT=8812 make test

    podman stop questdb
    podman rm questdb


Note that QuestDB is quite far from being compatible with the SQL understood by PostgreSQL (eg.
no TIME type, COUNT statements can't take an argument). 


## Testing the YugabyteDB distributed database

[YugabyteDB](https://yugabyte.com/) is an open source distributed database designed for large
volumes of data. It implements the PostgreSQL wire protocol.

    podman run --name yugabyte \
       --publish 5433:5433 \
       -d yugabytedb/yugabyte \
       bin/yugabyted start \
       --base_dir=/tmp \
       --daemon=false

    ELISP> (pg-connect "yugabyte" "yugabyte" "" "localhost" 5433)

    PGEL_DATABASE=yugabyte PGEL_USER=yugabyte PGEL_PASSWORD="" PGEL_PORT=5433 make test

    podman stop yugabyte
    podman rm yugabyte


## Testing with older Emacs versions

Docker/Podman images containing a range of old Emacs versions are maintained by Silex in the [docker-emacs
project](https://github.com/Silex/docker-emacs). Here's how to run them from podman while allowing
network access to your local host's network.

	cp ../pg.el test-pg.el ${WORKDIR}
	sudo podman run -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   silex/emacs:25.3 \
	   emacs -Q --batch -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test
