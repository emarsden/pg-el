EMACS ?= emacs
DOCKER ?= podman
WORKDIR := $(shell mktemp -d)

# export DEPS_DIR = $(shell realpath .deps)
export DEPS_DIR := $(shell mktemp -d -t emacsdepsXXXX)

default: test

# Note: we have to include test/ in test/install-deps.el because when we load ../pg.el we change the
# current directory to the parent of test.
.PHONY: install-deps
install-deps:
	${EMACS} -Q --batch ../pg.el -l test/install-deps.el

test: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el ../pg.el \
	   -l pg.el -l pg-geometry.el -l pg-gis.el -l pg-lo.el -l test-pg.el -f pg-test
	rm -r ${DEPS_DIR}

test-interactive: test-pg.el install-deps
	${EMACS} -Q -L .. -L . -l load-deps.el ../pg.el \
	   -l pg.el -l pg-geometry.el -l pg-gis.el -l test-pg.el -f pg-test

supabase: install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l supabase.el -f test-supabase

hang: install-deps
	${EMACS} -Q -L .. -L . -l load-deps.el -l ../pg.el -l hang.el

# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations (the PostgreSQL backend needs to be set up with TLS certificates), so this is a
# separate test target.
test-tls: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-tls

# This needs Emacs support for ALPN (currently under development).
test-tls-direct: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-tls-direct


# Run tests over a local Unix socket connection to PostgreSQL.
test-local: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-local

test-ebiacuk: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-ebiacuk


test-connections: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-connection-tests

test-multithreaded: test-multithreaded.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-multithreaded.el -f pgtest-multithreaded

test-async: test-async.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-async.el -f pgtest-async

test-pgvector: test-pgvector.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pgvector.el -f pg-test


# Using Docker images for Emacs from https://hub.docker.com/r/silex/emacs/ and the locally installed
# PostgreSQL.
test-emacs-dev: test-pg.el
	cp install-deps.el load-deps.el ../pg.el ../pg-geometry.el ../pg-gis.el ../pg-bm25.el test-pg.el ${WORKDIR}
	${DOCKER} run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:master-alpine-ci \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l ../tmp/pg-geometry.el -l /tmp/pg-gis.el -l /tmp/pg-bm25.el -l /tmp/test-pg.el -f pg-test

test-emacs28: test-pg.el
	cp install-deps.el load-deps.el ../pg.el ../pg-geometry.el ../pg-gis.el ../pg-bm25.el test-pg.el ${WORKDIR}
	${DOCKER} run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:28.1 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/pg-geometry.el -l /tmp/pg-gis.el -l /tmp/pg-bm25.el -l /tmp/test-pg.el -f pg-test

# The peg.el library doesn't actually compile with Emacs 27 (void-function byte-run--set-speed), so
# this doesn't work.
test-emacs27: test-pg.el
	cp install-deps.el load-deps.el ../pg.el test-pg.el ${WORKDIR}
	${DOCKER} run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:27.2 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# The extended query support will not work in Emacs versions before 28.1, because functionality
# provided by the bindat libary is needed. However, other functionality based on the simple query
# protocol (the pg-exec function) should work fine.
test-emacs26: test-pg.el
	cp install-deps.el load-deps.el ../pg.el test-pg.el ${WORKDIR}
	${DOCKER} run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true  \
	   docker.io/silex/emacs:26.3-alpine-ci \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# Emacs versions older than 26.1 will not work against a recent PostgreSQL version (that is set up
# to require SCRAM-SHA-256 authentication), because they don't include the GnuTLS support which we
# use to calculate HMACs. They may however work against a database set up to not require
# authentication for local connections.
test-emacs25: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	${DOCKER} run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:25.3 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# A special test for short usernames, database names, passwords and so on (we used to have some
# builtin assumptions that a username was a least 4 chars in length).
test-shortnames: test-pg.el
	${DOCKER} run --rm --name pgsql \
	   --publish 5926:5926 \
	   -e POSTGRES_DB=d \
	   -e POSTGRES_USER=u \
	   -e POSTGRES_PASSWORD=~ \
	   -e PGPORT=5926 \
	   -d docker.io/library/postgres:14
	sleep 5
	PGEL_PORT=5926 PGEL_DATABASE=d PGEL_USER=u PGEL_PASSWORD="~" $(MAKE) test
	${DOCKER} stop pgsql


# Note that we need to tell Emacs GnuTLS to trust the root.crt file in order for it to accept the
# PostgreSQL server TLS certificate (it's not signed by the system CA).
#
# Currently failing with Emacs 31.0.50 with
#   LOG:  received direct SSL connection request without ALPN protocol negotiation extension
test-postgresql18tls: test-pg.el
	openssl req -new -nodes -text -out ${WORKDIR}/root.csr -keyout ${WORKDIR}/root.key \
	   -subj "/CN=localhost"
	chmod og-rwx ${WORKDIR}/root.key
	openssl x509 -req -in ${WORKDIR}/root.csr -text -days 42 \
	   -extfile /etc/ssl/openssl.cnf -extensions v3_ca \
	   -signkey ${WORKDIR}/root.key -out ${WORKDIR}/root.crt
	openssl req -new -nodes -text -out ${WORKDIR}/server.csr -keyout ${WORKDIR}/server.key \
	   -subj "/CN=localhost"
	chmod og-rwx ${WORKDIR}/server.key
	openssl x509 -req -in ${WORKDIR}/server.csr -text -days 42 \
	   -CA ${WORKDIR}/root.crt -CAkey ${WORKDIR}/root.key \
	   -CAcreateserial -out ${WORKDIR}/server.crt
	openssl req -new -nodes -out ${WORKDIR}/client.csr -keyout ${WORKDIR}/client.key \
	   -subj "/CN=pgeltestuser"
	openssl x509 -req -days 42 -in ${WORKDIR}/client.csr \
	   -CA ${WORKDIR}/root.crt \
	   -CAkey ${WORKDIR}/root.key \
	   -CAcreateserial -out ${WORKDIR}/client.crt
	chmod 600 ${WORKDIR}/server.key
	ls -l ${WORKDIR}
	${DOCKER} volume create pgcerts
	tar cf ${WORKDIR}/certs.tar --directory=${WORKDIR} server.crt server.key root.crt
	${DOCKER} volume import pgcerts ${WORKDIR}/certs.tar
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
           -v pgcerts:/certs:U \
           --publish 5418:5418 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5418 \
           -e TZ=Asia/Tokyo \
           --user postgres \
           -d docker.io/library/postgres:18-alpine \
              -c ssl=on \
              -c ssl_cert_file=/certs/server.crt \
              -c ssl_key_file=/certs/server.key
	sleep 5
	PGEL_TRUST_CA=${WORKDIR}/root.crt PGEL_PORT=5418 $(MAKE) test-tls-direct
	${DOCKER} stop pgsql
	sleep 2
	${DOCKER} volume rm pgcerts


test-postgresql18: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
           --publish 5418:5418 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5418 \
           -e TZ=Asia/Tokyo \
           -d docker.io/library/postgres:18-alpine
	sleep 5
	PGEL_PORT=5418 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql17: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
	   --publish 5426:5426 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5426 \
	   -e TZ=Asia/Tokyo \
	   -d docker.io/library/postgres:17-alpine
	sleep 5
	PGEL_PORT=5426 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql16: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
	   --publish 5416:5416 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5416 \
	   -e TZ=Asia/Tokyo \
	   -d docker.io/library/postgres:16-alpine
	sleep 5
	TZ=Asia/Tokyo PGEL_PORT=5416 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql15: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
	   --publish 5116:5116 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5116 \
	   -d docker.io/library/postgres:15
	sleep 5
	PGEL_PORT=5116 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql14: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:14
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql13: test-pg.el
	${DOCKER} run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:13-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql12: test-pg.el
	${DOCKER} run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:12-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	${DOCKER} stop pgsql

test-postgresql11: test-pg.el
	${DOCKER} run --rm --name pgsql \
	   --publish 5437:5437 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5437 \
	   -d docker.io/library/postgres:11-alpine
	sleep 5
	PGEL_PORT=5437 $(MAKE) test
	${DOCKER} stop pgsql


# EDB with a Red Hat Universal Base Image. This defaults to an SQL_ASCII client-encoding unless we
# set LC_CTYPE.
#
# https://github.com/EnterpriseDB/docker-postgresql
test-enterprisedb: test-pg.el
	${DOCKER} run --rm --name edb \
	   --publish 5366:5366 \
	   -e TZ=UTC-7:00 \
	   -e LANG=en_US.UTF8 \
	   -e LC_CTYPE=en_US.UTF8 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5366 \
	   -d ghcr.io/enterprisedb/postgresql:17
	TZ=UTC-7:00 PGEL_PORT=5366 $(MAKE) test
	${DOCKER} stop edb


# Tests with a PostgreSQL server configured to only accept clients that present a CA-signed certificate.
#
#   https://www.postgresql.org/docs/current/ssl-tcp.html
#
# We use openssl to generate a new Root certificate authority and key. Use the root CA to create a
# server certificate and key and a client certificate and key. Start PostgreSQL with the server
# certificate and the root CA certificate, configured to require clients to present a client
# certificate signed by our root CA. Connect presenting the client certificate (this uses the GnuTLS
# support for client certificates in Emacs).
#
# Note: mounting the pgcerts volume with the :U modifier maps the file owner for all volume files to
# that selected by the container ("postgres"), which is required for PostgreSQL to accept that the
# server certificate key is not readable by other users.
#
# https://github.com/bitnami/containers/blob/main/bitnami/postgresql/README.md
#
# The Bitnami configuration for pg_hba.conf deletes all lines with authentication methods
# local, or md5, or trust (our value for POSTGRESQL_PHHBA_REMOVE_FILTERS), and adds a line
#
#   hostssl     all             all             0.0.0.0/0               cert
#
# This means the equivalent of clientcert=verify-full, meaning that the server will verify that the
# client certificate is signed by its root CA (configured as /certs/root.crt below) and will also
# verify that the username specified in the CN field of the certificate corresponds to the
# PostgreSQL username we are connecting as.
#
# The bitnami containers are also published to the AWS ECR, public.ecr.aws/bitnami/postgresql:latest
test-certificates: test-pg.el install-deps
	openssl req -new -nodes -text -out ${WORKDIR}/root.csr -keyout ${WORKDIR}/root.key \
	   -subj "/CN=localhost"
	chmod og-rwx ${WORKDIR}/root.key
	openssl x509 -req -in ${WORKDIR}/root.csr -text -days 42 \
	   -extfile /etc/ssl/openssl.cnf -extensions v3_ca \
	   -signkey ${WORKDIR}/root.key -out ${WORKDIR}/root.crt
	openssl req -new -nodes -text -out ${WORKDIR}/server.csr -keyout ${WORKDIR}/server.key \
	   -subj "/CN=localhost"
	chmod og-rwx ${WORKDIR}/server.key
	openssl x509 -req -in ${WORKDIR}/server.csr -text -days 42 \
	   -CA ${WORKDIR}/root.crt -CAkey ${WORKDIR}/root.key \
	   -CAcreateserial -out ${WORKDIR}/server.crt
	openssl req -new -nodes -out ${WORKDIR}/client.csr -keyout ${WORKDIR}/client.key \
	   -subj "/CN=pgeltestuser"
	openssl x509 -req -days 42 -in ${WORKDIR}/client.csr \
	   -CA ${WORKDIR}/root.crt \
	   -CAkey ${WORKDIR}/root.key \
	   -CAcreateserial -out ${WORKDIR}/client.crt
	chmod 600 ${WORKDIR}/server.key
	ls -l ${WORKDIR}
	${DOCKER} volume create pgcerts
	tar cf ${WORKDIR}/certs.tar --directory=${WORKDIR} server.crt server.key root.crt
	${DOCKER} volume import pgcerts ${WORKDIR}/certs.tar
	${DOCKER} run --rm --name pgsqltls \
	   -v pgcerts:/certs:U \
	   --publish 5488:5488 \
	   -e POSTGRESQL_PORT_NUMBER=5488 \
	   -e POSTGRESQL_DATABASE=pgeltestdb \
	   -e POSTGRESQL_USERNAME=pgeltestuser \
	   -e POSTGRESQL_PASSWORD=pgeltest \
	   -e POSTGRESQL_ENABLE_TLS=yes \
	   -e POSTGRESQL_TLS_CERT_FILE=/certs/server.crt \
	   -e POSTGRESQL_TLS_KEY_FILE=/certs/server.key \
	   -e POSTGRESQL_TLS_CA_FILE=/certs/root.crt \
	   -e POSTGRESQL_PGHBA_REMOVE_FILTERS=local,md5,trust \
           -e POSTGRESQL_INITDB_ARGS=--data-checksums \
           -d docker.io/bitnami/postgresql:latest
	sleep 5
	PGEL_CLIENT_CERT=${WORKDIR}/client.crt PGEL_CLIENT_CERT_KEY=${WORKDIR}/client.key PGEL_PORT=5488 ${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-client-cert
	${DOCKER} stop pgsqltls
	sleep 2
	${DOCKER} volume rm pgcerts


# https://hub.docker.com/r/stagex/user-postgresql
#
# This image is buggy: crun: executable file `/bin/sh` not found
test-stagex: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --pull=newer \
           --publish 5317:5317 \
           -e TZ=UTC-7:00 \
           -e LANG=en_US.UTF8 \
           -e LC_CTYPE=en_US.UTF8 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5317 \
           -d quay.io/stagex/user-postgresql:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5317/pgeltestdb $(MAKE) test
	${DOCKER} stop pgsql


# Supabase (https://supabase.com) provide hosted PostgreSQL instances with convenient web
# dashboards, management APIs and integrations with authentication libraries. They have a free tier.
# As of 2023-08, they are running PostgreSQL 15.1 on Aarch64. As of 2024-09, their TLS setup is not
# compatible with the GnuTLS settings used by the Emacs TLS support, and Emacs is not able to
# connect.
test-supabase: test-pg.el
	$(MAKE) test-tls

# Neon (https://neon.tech/) provide hosted "serverless" PostgreSQL instances, which allow convenient
# automated scalability according to load. They have a free tier. As of 2024-09, they are running
# PostgreSQL "15.8" on AMD64.
test-neon: test-pg.el
	PGURI="postgresql://user:password@foo.eu-central-1.aws.neon.tech/main?sslmode=require" $(MAKE) test-tls


# https://github.com/neondatabase/neon/blob/main/docs/docker.md
#
# need docker-compose / podman-compose
# https://github.com/neondatabase/neon/blob/main/docker-compose/docker-compose.yml
test-neon-local: test-pg.el
	${DOCKER} run --rm --name neon \
          --pull=newer \
          --publish 5662:5432 \
          docker.io/neondatabase/neon
	sleep 5
	${DOCKER} stop neon


# app.xata.io are running PostgreSQL 15.5 on aarch64/Linux as of 2025-01
test-xata: test-pg.el
	PGURI="postgresql://user:password@eu-central-1.sql.xata.sh/pgeltestdb:main?sslmode=force" $(MAKE) test-tls


# https://docs.tigerdata.com/self-hosted/latest/install/installation-docker/
#
# Note that Timescaledb has a bunch of internal tables (owned by the currently connected user) in
# schemas named _timescaledb_cache, _timescaledb_catalog, timescaledb_internal and so on. We have
# code in pg-el to remove these from the list of tables returned by pg-tables.
test-timescale: test-pg.el
	${DOCKER} run --rm --name timescale \
           --pull=newer \
           --publish 5981:5981 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5981 \
           -d docker.io/timescale/timescaledb-ha:pg18-ts2.24-oss
	sleep 5
	PGEL_PORT=5981 $(MAKE) test
	${DOCKER} stop timescale

#   -d docker.io/timescale/timescaledb:latest-pg17


# https://hub.docker.com/r/citusdata/citus
#
# An extension for sharding PostgreSQL. We could recognize it by checking for the system tables
# s(pg-qualified-name columnar_internal options) #s(pg-qualified-name columnar_internal stripe)
# #s(pg-qualified-name columnar_internal chunk_group) #s(pg-qualified-name columnar_internal chunk),
# or by the presence of "citus_internal" in the pg-schemas list, but since we don't see any
# behavioural difference from standard PostgreSQL, we don't report this as a variant.
test-citus: test-pg.el
	${DOCKER} run --rm --name citus \
           --pull=newer \
           --publish 127.0.0.1:5409:5409 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5409 \
           -d docker.io/citusdata/citus:latest
	sleep 5
	PGEL_PORT=5409 $(MAKE) test
	${DOCKER} stop citus


# A PostgreSQL extension that adds a new storage engine designed for better multithreading and solid
# state storage.
#
# https://github.com/orioledb/orioledb
# https://hub.docker.com/r/orioledb/orioledb
#
# Note: the documentation states that we can use -c default_table_access_method=orioledb in
# INITDB_ARGS, but that leads to a fatal error "access method "orioledb" does not exist".
test-orioledb: test-pg.el
	${DOCKER} run --rm --name orioledb \
           --pull=newer \
           --publish 127.0.0.1:5317:5317 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5317 \
           -e POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en" \
           -e LANG=en_US.UTF8 \
           -e LC_CTYPE=en_US.UTF8 \
           -d docker.io/orioledb/orioledb:latest-pg17
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5317/pgeltestdb $(MAKE) test
	${DOCKER} stop orioledb


# https://github.com/duckdb/pg_duckdb
test-pgduckdb: test-pg.el
	${DOCKER} run --rm --name pgduckdb \
           --pull=newer \
           --publish 127.0.0.1:5317:5317 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5317 \
           -e LANG=en_US.UTF8 \
           -e LC_CTYPE=en_US.UTF8 \
           -d docker.io/pgduckdb/pgduckdb:18-v1.1.1
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5317/pgeltestdb $(MAKE) test
	${DOCKER} stop pgduckdb


# An Oracle-compatible flavour of PostgreSQL.
#
# https://docs.ivorysql.org/en/ivorysql-doc/v3.4/v3.4/6#Docker-installation
# https://registry.hub.docker.com/r/ivorysql/ivorysql/tags
test-ivorysql: test-pg.el
	${DOCKER} run --rm --name ivorydb \
          --pull=newer \
	  --publish 127.0.0.1:5437:5432 \
	  -e LANG=C.UTF8 \
	  -e LC_CTYPE=C.UTF8 \
	  -e IVORYSQL_DB=pgeltestdb \
	  -e IVORYSQL_USER=pgeltestuser \
	  -e IVORYSQL_PASSWORD=pgeltest \
	  -d docker.io/ivorysql/ivorysql:5.0-ubi8
	sleep 5
	PGEL_PORT=5437 $(MAKE) test
	${DOCKER} stop ivorydb

# An ElasticSearch alternative, built as a PostgreSQL extension. Works fine with pg.el.
# https://docs.paradedb.com/introduction#get-started
#
# As of 2025-05, the paradedb docker image is built on a Debian PostgreSQL 17.4. It has some TIGER
# data preloaded.
test-paradedb: test-pg.el
	${DOCKER} run --rm --name paradedb \
          --pull=newer \
	  --publish 5441:5441 \
          -e POSTGRES_DB=pgeltestdb \
          -e POSTGRESQL_PORT_NUMBER=5441 \
          -e PGPORT=5441 \
          -e POSTGRESQL_MASTER_PORT_NUMBER=5441 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e POSTGRESQL_POSTGRES_PASSWORD=pgeltest \
          -d docker.io/paradedb/paradedb:latest
	sleep 5
	PGEL_PORT=5441 $(MAKE) test
	${DOCKER} stop paradedb

# Another ElasticSearch alternative, built as a PostgreSQL extension, implementing the BM25 algorithm.
#
# https://github.com/tensorchord/VectorChord-bm25/
test-vectorchord: test-pg.el
	${DOCKER} run --rm --name vectorchord \
          --pull=newer \
          --publish 5778:5432 \
          -e POSTGRES_DB=pgeltestdb \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -d ghcr.io/tensorchord/vchord_bm25-postgres:pg17-v0.2.1
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@localhost:5778/pgeltestdb $(MAKE) test
	${DOCKER} stop vectorchord

# Hydra Columnar database
# https://github.com/hydradatabase/columnar?tab=readme-ov-file
test-hydra: test-pg.el
	${DOCKER} run --rm --name hydra \
          --pull=newer \
          --publish 5167:5167 \
          -e POSTGRES_DB=pgeltestdb \
          -e POSTGRESQL_PORT_NUMBER=5167 \
          -e PGPORT=5167 \
          -e POSTGRESQL_MASTER_PORT_NUMBER=5167 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e POSTGRESQL_POSTGRES_PASSWORD=pgeltest \
          -d ghcr.io/hydradatabase/hydra:15-5a258d26896de0f47b08658dc8fa914ad453eeab
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@localhost:5167/pgeltestdb $(MAKE) test
	${DOCKER} stop hydra


# Apache AGE is a graph query extension for PostgreSQL.
#
# https://age.apache.org/getstarted/quickstart
test-age: test-pg.el
	${DOCKER} run --rm --name age \
          --pull=newer \
          -p 5455:5432 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e POSTGRES_DB=pgeltestdb \
          -d docker.io/apache/age
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5455/pgeltestdb $(MAKE) test
	${DOCKER} stop age


# An open source graph database based on PostgreSQL. We detect this as PostgreSQL because its
# behaviour does not seem to deviate from standard PostgreSQL.
#
# https://github.com/skaiworldwide-oss/agensgraph
test-agensgraph: test-pg.el
	${DOCKER} run --rm --name agensgraph \
          --pull=newer \
          --publish 127.0.0.1:5455:5432 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e POSTGRES_DB=pgeltestdb \
          -d docker.io/skaiworldwide/agensgraph:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5455/pgeltestdb $(MAKE) test
	${DOCKER} stop agensgraph


# https://cedardb.com/docs/
test-cedardb: test-pg.el
	${DOCKER} run --rm --name cedardb \
           --pull=newer \
           --publish 127.0.0.1:6778:5432 \
           -e CEDAR_USER=pgeltestuser \
           -e CEDAR_PASSWORD=pgeltest \
           -e CEDAR_DB=pgeltestdb \
           -d docker.io/cedardb/cedardb:latest
	sleep 10
	PGURI="postgresql://pgeltestuser:pgeltest@127.0.0.1:6778/pgeltestdb?sslmode=disable" $(MAKE) test
	${DOCKER} stop cedardb


# https://hub.docker.com/r/pgedge/pgedge
# https://github.com/pgEdge/postgres-images/pkgs/container/pgedge-postgres
# https://github.com/pgEdge/pgedge-docker
test-pgedge: test-pg.el
	${DOCKER} run --rm --name pgedge \
           --pull=newer \
           --publish 127.0.0.1:5481:5432 \
           -v "./pgedge-config.json:/home/pgedge/db.json" \
           -e PGPORT=5432 \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e POSTGRES_DB=pgeltestdb \
           -e LANG=en_US.UTF8 \
           -e LC_CTYPE=en_US.UTF8 \
           -d ghcr.io/pgedge/pgedge-postgres:18-spock5-minimal -c listen_addresses="*"
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5481/pgeltestdb $(MAKE) test
	${DOCKER} stop pgedge


# Microsoft DocumentDB (PostgreSQL-based). This is not the same product as Amazon DocumentDB.
#
# https://github.com/microsoft/documentdb
#
# Here using the prebuilt Docker image provided by FerretDB
# https://docs.ferretdb.io/installation/ferretdb/docker/
#
#            -e POSTGRES_INITDB_ARGS="-c cron.database_name=pgeltestdb" \

# Note: we can't change the database name with POSTGRES_DATABASE as that messes with the
# initialization of the pg_cron extension. Likewise for the POSTGRES_USER because that changes the
# default database name.
test-documentdb: test-pg.el
	${DOCKER} run --rm --name documentdb \
           --pull=newer \
           --publish 5488:5488 \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5488 \
           -d ghcr.io/ferretdb/postgres-documentdb:latest
	sleep 5
	PGURI=postgresql://postgres:pgeltest@127.0.0.1:5488/postgres $(MAKE) test
	${DOCKER} stop documentdb


# https://babelfishpg.org/
# https://hub.docker.com/r/jonathanpotts/babelfishpg
#
# This behaves exactly like PostgreSQL (which it is forked from).
test-babelfish: test-pg.el
	${DOCKER} run --rm --name babelfish \
          --publish 127.0.0.1:5410:5432 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e POSTGRES_DB=pgeltestdb \
          -d docker.io/jonathanpotts/babelfishpg:latest
	sleep 5
	PGURI=postgresql://babelfish_user:12345678@127.0.0.1:5410/postgres $(MAKE) test
	${DOCKER} stop babelfish


# Test pgbouncer proxying to our local PostgreSQL (in session pooling mode, so that functionality
# such as LISTEN works).
#
# https://github.com/edoburu/docker-pgbouncer
#
# TODO: need to move away from the unmaintained bitnami Docker images, perhaps to ghcr.io/icoretech/pgbouncer-docker
test-pgbouncer-old: test-pg.el
	${DOCKER} run --rm --name pgbouncer \
	  --net host \
	  -e PGBOUNCER_PORT=6432 \
	  -e PGBOUNCER_DATABASE=pgeltestdb \
	  -e POSTGRESQL_USERNAME=pgeltestuser \
	  -e POSTGRESQL_PASSWORD=pgeltest \
	  -e POSTGRESQL_HOST=localhost \
	  -e POSTGRESQL_PORT=5432 \
	  -d docker.io/bitnami/pgbouncer:latest
	sleep 5
	PGEL_PORT=6432 $(MAKE) test
	${DOCKER} stop pgbouncer


# https://github.com/icoretech/pgbouncer-docker/pkgs/container/pgbouncer-docker
test-pgbouncer: test-pg.el
	${DOCKER} run --rm --name pgbouncer \
          -v ./pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini \
          -v ./pgbouncer-auth-file.txt:/etc/pgbouncer/pgbouncer-auth-file.txt \
          --net host \
          -d ghcr.io/icoretech/pgbouncer-docker:1.25.0
	sleep 5
	PGEL_PORT=6432 $(MAKE) test
	${DOCKER} stop pgbouncer



# pgcat bouncer/sharding middleware
#
# https://github.com/postgresml/pgcat
# https://github.com/postgresml/pgcat/blob/main/Dockerfile
test-pgcat: test-pg.el
	${DOCKER} run --rm --name pgsql \
           --publish 5437:5437 \
           -e POSTGRES_DB=pgeltestdb \
           -e POSTGRES_USER=pgeltestuser \
           -e POSTGRES_PASSWORD=pgeltest \
           -e PGPORT=5437 \
           -d docker.io/library/postgres:17-alpine
	sleep 5
	${DOCKER} run --rm --name pgcat \
           --pull=newer \
           -v ./pgcat-config.toml:/etc/pgcat/pgcat.toml \
           --net host \
           -d ghcr.io/postgresml/pgcat:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:6432/pgeltestdb?read_timeout=1000 $(MAKE) test
	${DOCKER} stop pgcat
	${DOCKER} stop pgsql


# pgdog bouncer/sharding middleware (same developer as pgcat)
#
# https://github.com/pgdogdev/pgdog
test-pgdog: test-pg.el
	cp pgdog-config.toml pgdog-users.toml /tmp
	${DOCKER} run --rm --name pgdog \
           --pull=newer \
	   -v /tmp/pgdog-config.toml:/pgdog/pgdog.toml \
	   -v /tmp/pgdog-users.toml:/pgdog/users.toml \
	   --net host \
	   -d ghcr.io/pgdogdev/pgdog:main
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5469/pgeltestdb $(MAKE) test
	${DOCKER} stop pgdog


# Yandex Odyssey connection pooler
#
# https://github.com/yandex/odyssey
# https://hub.docker.com/r/riftbit/yandex-odyssey
test-odyssey: test-pg.el
	${DOCKER} run --rm --name odyssey \
           --pull=newer \
           -v $(shell pwd)/setup-odyssey.conf:/etc/odyssey/odyssey.conf \
           --net host \
           -d docker.io/riftbit/yandex-odyssey:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5633/pgeltestdb $(MAKE) test
	${DOCKER} stop odyssey


# https://supabase.github.io/supavisor/
#
# Using it in session mode because this is the only mode which supports prepared statements.
#
# We start up two PostgreSQL servers: one as a metadata store for the tenants, used by supavisor,
# and one to be proxied by supavisor.
test-supavisor: test-pg.el
	${DOCKER} run --rm --name supametastore \
           --pull=newer \
           --publish 127.0.0.1:5488:5488 \
           -e POSTGRESQL_PORT_NUMBER=5488 \
           -e POSTGRESQL_DATABASE=metastore \
           -e POSTGRESQL_USERNAME=metastore \
           -e POSTGRESQL_PASSWORD=metastore \
           -d docker.io/bitnami/postgresql:latest
	sleep 5
	curl -L https://raw.githubusercontent.com/supabase/supavisor/refs/heads/main/dev/postgres/00-setup.sql \
        | psql postgresql://metastore:metastore@127.0.0.1:5488/metastore
	${DOCKER} run --rm --name supavised \
           --pull=newer \
           --publish 127.0.0.1:5489:5489 \
           -e POSTGRESQL_PORT_NUMBER=5489 \
           -e POSTGRESQL_DATABASE=pgeltestdb \
           -e POSTGRESQL_USERNAME=pgeltestuser \
           -e POSTGRESQL_PASSWORD=pgeltest \
           -d docker.io/bitnami/postgresql:latest
	sleep 5
	${DOCKER} run --rm --name supavisor \
           --pull=newer \
           --publish 127.0.0.1:5398:5432 \
           --publish 127.0.0.1:4000:4000 \
           -e PROXY_PORT_SESSION=5452 \
           -e SECRET_KEY_BASE=`openssl rand -base64 48` \
           -e VAULT_ENC_KEY=`openssl rand -base64 24` \
           -e API_JWT_SECRET="foo" \
           -e REGION=local \
           -e ERL_AFLAGS="-proto_dist inet_tcp" \
           -e DATABASE_URL=ecto://metastore:metastore@127.0.0.1:5488/metastore \
           -d docker.io/supabase/supavisor:2.6.1
	sleep 5
	# create a tenant in supavisor (the supervised database)
	curl -i -X PUT \
  'http://127.0.0.1:4000/api/tenants/pgeltest' \
  --header 'Accept: */*' \
  --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.wLKpjVW9IRyDIg2Wsrg9_vQ1oo3fk1Hsdi7RmP33giY' \
  --header 'Content-Type: application/json' \
  --data-raw @supavisor-newtenant.json
	PGURI=postgresql://pgeltestuser.pgeltest:pgeltest@127.0.0.1:5452/pgeltestdb $(MAKE) test
	${DOCKER} stop supavisor



# https://readyset.io/docs/get-started/install-rs/docker/postgres
# https://readyset.io/docs/reference/cli/readyset
test-readyset: test-pg.el
	${DOCKER} run --rm --name bitnamipg \
           --pull=newer \
	   --publish 127.0.0.1:5488:5488 \
	   -e POSTGRESQL_PORT_NUMBER=5488 \
	   -e POSTGRESQL_DATABASE=pgeltestdb \
	   -e POSTGRESQL_USERNAME=pgeltestuser \
	   -e POSTGRESQL_PASSWORD=pgeltest \
	   -d docker.io/bitnami/postgresql:latest
	sleep 5
	${DOCKER} run --rm --name readyset \
          --pull=newer --net=host \
          -e DISABLE_UPSTREAM_SSL_VERIFICATION=true \
          -e UPSTREAM_DB_URL=postgresql://pgeltestuser:pgeltest@localhost:5488/pgeltestdb \
          -e LISTEN_ADDRESS=127.0.0.1:5433 \
          -d docker.io/readysettech/readyset:latest --disable-telemetry --database-type=postgresql
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5433/pgeltestdb $(MAKE) test
	${DOCKER} stop readyset
	${DOCKER} stop bitnamipg



# CrateDB uses a default database name of "doc" that we can't set via Docker. Doesn't get very far
# through the tests: CrateDB doesn't accept a query which only contains an SQL comment, and doesn't
# implement the BYTEA, JSON, JSONB and HSTORE types, doesn't support COPY, doesn't support Unicode
# identifiers.
#
# CrateDB is fantastically slow on some CREATE TABLE statements that include a VARCHAR(n) column,
# which is why we set the read_timeout to a high value.
#
# https://hub.docker.com/_/crate/
# https://github.com/crate/docker-crate/blob/master/Dockerfile
# https://cratedb.com/docs/guide/install/container/docker.html
# https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html
test-cratedb: test-pg.el
	${DOCKER} run --rm --name cratedb \
          --pull=newer \
          --net host \
          -e CRATE_HEAP_SIZE=1g \
          -d docker.io/crate:latest -Cnetwork.host=127.0.0.1 -Cdiscovery.type=single-node -Cpsql.port=5789
	sleep 5
	PGURI="postgresql://crate@127.0.0.1:5789/postgres?read_timeout=1000" $(MAKE) test
	${DOCKER} stop cratedb


# Doesn't get very far through the tests, with an internal error generated by our query for
# pg-table-owner, and failing on the boolean vector syntax b'1001000'.
#
# Still affected as of 2024-12 by the panic bug https://github.com/cockroachdb/cockroach/issues/104009
# which was reported in May 2023!
test-cockroach: test-pg.el
	${DOCKER} run --rm --name cockroachdb \
          --pull=newer \
          --publish 26257:26257 \
          -e COCKROACH_DATABASE=pgeltestdb \
          -e COCKROACH_USER=pgeltestuser \
          -e COCKROACH_PASSWORD=pgeltest \
          -d docker.io/cockroachdb/cockroach start-single-node
	sleep 10
	PGURI="postgresql://pgeltestuser:pgeltest@localhost:26257/pgeltestdb?sslmode=require" $(MAKE) test



# This database (based on PostgreSQL 12.3 but which for some reason claims to have a backend major
# version of 11) doesn't get very far through the tests; it fails on "SELECT 1::integer" because it
# doesn't recognize integer as a type. Also, DELETE statements are not supported.
#
# Note: we could test the TLS support for the PostgreSQL wire protocol by setting parameters
#   tls.enabled=true
#   tls.demo.mode=true
# 
# https://questdb.io/docs/configuration/
test-questdb: test-pg.el
	${DOCKER} run --rm --name questdb \
          --pull=newer \
          --publish 127.0.0.1:8812:8812 \
          --publish 127.0.0.1:9000:9000 \
          -e QDB_PG_USER=pgeltestuser \
          -e QDB_PG_PASSWORD=pgeltest \
	  -d docker.io/questdb/questdb:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:8812/postgres $(MAKE) test
	${DOCKER} stop questdb


# https://docs.geldata.com/reference/running/deployment/docker
# https://docs.geldata.com/reference/running/configuration
# https://hub.docker.com/r/geldata/gel
# https://github.com/geldata/gel
#
# We don't bother detecting this as a specific PostgreSQL variant, because the SQL syntax it accepts
# is too different from standard PostgreSQL (no support for CREATE, for DROP, for EXPLAIN...)
test-geldata: test-pg.el
	${DOCKER} run --rm --name geldata \
          --pull=newer \
          --publish 127.0.0.1:5656:5656 \
          -e GEL_SERVER_USER=pgeltestuser \
          -e GEL_SERVER_PASSWORD=pgeltest \
          -e GEL_SERVER_PORT=5656 \
          -e GEL_SERVER_SECURITY=insecure_dev_mode \
          -d docker.io/geldata/gel:nightly
	sleep 20
	PGURI="postgresql://pgeltestuser:pgeltest@127.0.0.1:5656/edgedb?sslmode=require" $(MAKE) test-tls
	${DOCKER} stop geldata


# Materialize (here the "Materialize emulator" running in Docker). This proprietary differential
# dataflow database has many limitations in its PostgreSQL compatibility: no support for primary
# keys, unique constraints, check constraints, for the 'bit' type for example.
#
# https://materialize.com/blog/postgres-compatibility/
test-materialize: test-pg.el
	${DOCKER} run --rm --name materialize \
          --pull=newer \
          --publish 127.0.0.1:6875:6875 \
          -d docker.io/materialize/materialized:latest
	sleep 10
	PGURI=postgresql://materialize@127.0.0.1:6875/materialize $(MAKE) test
	${DOCKER} stop materialize


# Yellowbrick Database Community Edition (requires Kubernetes, currently untested)
#
# https://docs.yellowbrick.com/7.1.0/platforms/cloud/ce/docker/ce_cloud_docker.html
# https://hub.docker.com/r/yellowbrickdata/ybd-ce-k0s-amd64
test-yellowbrick-ce: test-pg.el
	${DOCKER} volume ls --format '{{ .Name }}' | grep -qw ybd-storage-rfs || podman volume create ybd-storage-rfs
	${DOCKER} volume ls --format '{{ .Name }}' | grep -qw ybd-storage-lfs || podman volume create ybd-storage-lfs
	mkdir -p /tmp/yellowbrick
	${DOCKER} run --rm --name yellowbrick \
          --hostname yellowbrick \
	  --cgroupns=host \
	  -v ybd-storage-rfs:/var/lib/k0s \
	  -v ybd-storage-lfs:/tmp/yellowbrick \
	  -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
	  --publish 5689:5432 \
          -d docker.io/yellowbrickdata/ybd-ce-k0s-amd64:stable
	sleep 100
	PGURI=postgresql://postgres@127.0.0.1:5689/postgres $(MAKE) test
	${DOCKER} stop yellowbrick
	${DOCKER} volume rm ybd-storage-rfs
	${DOCKER} volume rm ybd-storage-lfs

# https://hub.docker.com/r/yellowbrickdata/yb-community-edition
#
# The YM_OFF option disables the web interface. The cluster manager is extremely slow to start
# up sometimes, so we wait a long time before the test.
test-yellowbrick: test-pg.el
	${DOCKER} run --rm --name yellowbrick \
         --publish 127.0.0.1:5689:5432 \
         -e YB_ADMIN_PASSWORD=pgeltest \
         -d docker.io/yellowbrickdata/yb-community-edition:latest --YM_OFF
	sleep 200
	psql postgresql://ybdadmin:pgeltest@127.0.0.1:5689/yellowbrick -c "CREATE DATABASE pgeltestdb WITH ENCODING 'UTF8';"
	sleep 1
	PGURI="postgresql://ybdadmin:pgeltest@127.0.0.1:5689/pgeltestdb?read_timeout=100" $(MAKE) test
	${DOCKER} stop yellowbrick



# https://docs.immudb.io/master/running/download.html
# https://hub.docker.com/r/codenotary/immudb
#
# immadmin doesn't seem to provide a useful way to provide password via environment
# not to be used in a non-interactive manner
#
# We see a protocol error "database name not provided" after the StartupMessage
# whereas psql is able to connect when disabling TLS
#   psql "sslmode=disable host=localhost port=5667 dbname=pgeltestdb user=pgeltestuser password=pgeltest"
# though few SQL commands work.
test-immudb: test-pg.el
	${DOCKER} run --rm --name immudb \
          --net host \
          -e IMMUDB_PGSQL_SERVER=true \
          -e IMMUDB_ADMIN_PASSWORD=pgeltest \
          -e USER=pgeltestuser \
          -d docker.io/codenotary/immudb:latest \
            --pgsql-server --pgsql-server-port 5667 \
            --admin-password pgeltest --force-admin-password
	sleep 2
	echo pgeltest|${DOCKER} exec -ti immudb immuadmin login immudb
	${DOCKER} exec -ti immudb immuadmin database create pgeltestdb
	expect -c 'spawn ${DOCKER} exec -ti immudb immuadmin user create pgeltestuser readwrite pgeltestdb;sleep 0.1;expect "Choose a password for";sleep 0.1;send "pgeltest\r";expect "continue with your password instead.*Y/n";sleep 0.1;send "Y\r";expect "Confirm password";sleep 0.1;send "pgeltest\r";sleep 0.1;exit'
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5667/pgeltestdb $(MAKE) test
	${DOCKER} stop immudb


# https://hub.docker.com/r/andruche/greenplum
# https://github.com/greenplum-db/gpdb-archive
test-greenplum: test-pg.el
	${DOCKER} run --rm --name greenplum \
          --pull=newer \
          --publish 127.0.0.1:5433:5432 \
          -e PGPORT=5433 \
          -d docker.io/andruche/greenplum:7
	sleep 10
	PGURI=postgresql://gpadmin@localhost:5433/postgres $(MAKE) test
	${DOCKER} stop greenplum


# https://github.com/woblerr/docker-greenplum
#
# This is failing with Podman in user mode with "/usr/bin/ping6: raw socket: Operation not permitted"
#
# TODO: perhaps try also https://hub.docker.com/r/qpod/greenplum
test-greenplum2: test-pg.el
	${DOCKER} run --rm --name greenplum \
          --pull=newer \
          --publish 127.0.0.1:5433:5432 \
          -e GREENPLUM_DEPLOYMENT=singlenode \
          -e GREENPLUM_DATABASE_NAME=pgeltestdb \
          -e GREENPLUM_PASSWORD=pgeltest \
          ghcr.io/woblerr/greenplum:7.1.0
	PGURI=postgresql://gpadmin:pgeltest@127.0.0.1:5433/pgeltestdb $(MAKE) test
	${DOCKER} stop greenplum


# The Cloudberry fork of Greenplum, incubated at Apache.
# https://github.com/apache/cloudberry/
# https://hub.docker.com/r/apache/incubator-cloudberry/
#
# No prebuilt Docker image seems to be available.
test-cloudberry: test-pg.el
	${DOCKER} run --rm --name cloudberry \
          --publish 127.0.0.1:5661:5661 \
          -h cdw -e PGPORT=5661 \
          -d docker.io/apache/incubator-cloudberry:cbdb-test-rocky9-latest
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=gpadmin PGEL_PASSWORD="greenplum" PGEL_PORT=5661 $(MAKE) test
	${DOCKER} stop cloudberry


test-cloudberry2: test-pg.el
	${DOCKER} run --rm --name cloudberry \
          --publish 127.0.0.1:5661:5661 \
          -h cdw -e PGPORT=5661 \
          docker.io/scottdolecki/cloudberry:latest
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=gpadmin PGEL_PASSWORD="greenplum" PGEL_PORT=5661 $(MAKE) test
	${DOCKER} stop cloudberry


# Google Spanner emulator
# See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/emulator.md
# https://cloud.google.com/spanner/docs/pgadapter-start#docker
#
# Extremely limited PostgreSQL support. Basic types such as int2 are not supported.
test-spanner: test-pg.el
	${DOCKER} run --rm --name spanner \
          --pull=newer \
          --publish 127.0.0.1:5499:5432 \
          -d gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator:latest -d pgeltestdb
	sleep 2
	PGURI=postgresql://postgres@127.0.0.1:5499/pgeltestdb $(MAKE) test
	${DOCKER} stop spanner


# YDB by Yandex
# 
# https://ydb.tech/docs/en/postgresql/docker-connect
# https://hub.docker.com/r/ydbplatform/local-ydb/tags (this an older version)
#
# Fairly limited PostgreSQL support currently; for example all tables must have a primary key.
# Bit vectors are not returned with a bitvector oid in the metainformation.
#
# Many supported functions, as per https://ydb.tech/docs/en/postgresql/functions
#
# There is apparently an environment variable YDB_PG_PORT but it seems to be ignored.
test-ydb: test-pg.el
	${DOCKER} run --rm --name ydb \
          --pull=newer \
          --publish 127.0.0.1:5119:5432 \
          -e YDB_EXPERIMENTAL_PG=1 \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -e YDB_PG_DATABASE=pgeltestdb \
          -e YDB_USE_IN_MEMORY_PDISKS=true \
          -e YDB_FEATURE_FLAGS=enable_temp_tables \
          -d docker.io/ydbplatform/local-ydb:nightly
	sleep 5
	PGURI="postgresql://pgeltestuser:pgeltest@localhost:5119/local?read_timeout=100" $(MAKE) test
	${DOCKER} stop ydb

# ghcr.io/ydb-platform/local-ydb:nightly



# Yugabyte 2.25 is based on PostgreSQL 15.2. This works very well, including the HSTORE and pgvector
# extensions. The sequence test fails (SELECT last_value FROM pg_sequences). Some more recent SQL
# evolutions such as "INTEGER GENERATED ALWAYS AS expression STORED" are not supported.
# LISTEN/NOTIFY is not supported.
#
# TODO: test YSQL_PASSWORD
# https://docs.yugabyte.com/preview/reference/configuration/yugabyted/
test-yugabyte: test-pg.el
	${DOCKER} run --rm --name yugabyte \
          --net host --pull=newer \
          -e YSQL_USER=pgeltestuser \
          -e YSQL_PASSWORD=pgeltest \
          -e YSQL_DB=pgeltestdb \
          -d docker.io/yugabytedb/yugabyte:latest \
            bin/yugabyted start \
            --advertise_address 127.0.0.1 \
            --ysql_port=5499 \
            --base_dir=/tmp \
            --enable_pg_parity_early_access \
            --ysql_enable_auth=true \
            --background=false
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5499/pgeltestdb $(MAKE) test
	${DOCKER} stop yugabyte


# https://github.com/ApsaraDB/PolarDB-for-PostgreSQL from Alibaba Cloud
#
# Note: primary runs on $POLARDB_PORT, replicas on ${POLARDB_PORT}+1 and ${POLARDB_PORT}+2
# We don't seem to be able to set POLARDB_DATABASE or equivalent
test-polardb: test-pg.el
	${DOCKER} run --rm --name polardb \
          --pull=newer \
          --publish 127.0.0.1:5998:5998 \
          -e POLARDB_PORT=5998 \
          -e POLARDB_USER=pgeltestuser \
          -e POLARDB_PASSWORD=pgeltest \
          -d docker.io/polardb/polardb_pg_local_instance:15
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5998/postgres $(MAKE) test
	${DOCKER} stop polardb


# Huawai openGauss fork of PostgreSQL
#
# https://hub.docker.com/r/opengauss/opengauss
test-opengauss: test-pg.el
	${DOCKER} run --rm --name opengauss \
           --pull=newer \
           --publish 127.0.0.1:5488:5488 \
           -e TZ=UTC-7:00 \
           -e LANG=en_US.UTF8 \
           -e LC_CTYPE=en_US.UTF8 \
           -e GS_PASSWORD="PGelt44^" \
           -e GS_USERNAME=pgeltestuser \
           -e GS_PORT=5488 \
           -d docker.io/opengauss/opengauss:7.0.0-RC2.B015
	sleep 20
	PGURI=postgresql://pgeltestuser:PGelt44^@127.0.0.1:5488/postgres $(MAKE) test
	${DOCKER} stop opengauss


# AntDB fork of PostgreSQL
# https://hub.docker.com/r/falconia/antdb-ce
test-antdb: test-pg.el
	${DOCKER} run --rm --name antdb \
           --pull=newer \
           --publish 127.0.0.1:5677:5432 \
           -e ANTDB_DB=pgeltestdb \
           -e ANTDB_USER=pgeltestuser \
           -e ANTDB_PASSWORD=pgeltest \
           -e ANTDB_LISTEN_ADDRESSES="*" \
	   -d docker.io/falconia/antdb-ce:latest
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5677/pgeltestdb $(MAKE) test
	${DOCKER} stop antdb


# Very limited PostgreSQL support: there is no pg_type table so we can't retrieve information
# regarding the OID of builtin types. We have to be careful during the initialization sequence not
# to send the query "SET datestyle = 'ISO'", which would fail and cause the network connection to be
# reset (!).
# 
# Default values for config.xml file at
# https://github.com/ClickHouse/ClickHouse/blob/c7996d54536492ebd4f436672e466464e8474ff9/programs/server/config.xml
test-clickhouse: test-pg.el
	echo '<clickhouse><postgresql_port>5491</postgresql_port><core_dump><size_limit>0</size_limit></core_dump><tcp_connection_close>0</tcp_connection_close></clickhouse>' > /tmp/pgel-config.xml
	${DOCKER} run --rm --name clickhouse \
	   --ulimit nofile=62144:62144 \
	   --ulimit core=234567:234567 \
	   -v /tmp/pgel-config.xml:/etc/clickhouse-server/config.d/pgel-config.xml \
	   --publish 127.0.0.1:5491:5491 \
	   --publish 127.0.0.1:18123:8123 \
	   --publish 127.0.0.1:9000:9000 \
	   -e CLICKHOUSE_DB=pgeltestdb \
	   -e CLICKHOUSE_USER=pgeltestuser \
	   -e CLICKHOUSE_PASSWORD=pgeltest \
	   -d docker.io/clickhouse/clickhouse-server:latest
	sleep 5
	PGEL_PORT=5491 PGEL_SERVER_VARIANT=clickhouse $(MAKE) test
	${DOCKER} stop clickhouse


# https://github.com/timeplus-io/proton (Apache 2 licence)

# This stream processing database is built on ClickHouse and suffers from the same defects as it.
#
# We are currently not detecting it as a dedicated variant. Perhaps could query information_schema.schemata
# or if we have a table system.crash_log it could be assumed to be ClickHouse derived.
#
# See https://github.com/timeplus-io/proton/blob/a67dc38f072ca7ff663bd56c285983426ab4b169/docker/server/entrypoint.sh#L52
#
#           -e PROTON_USER=pgeltestuser 
#           -e PROTON_DB=pgeltestdb 
#           -e PROTON_PASSWORD=pgeltest 
test-proton: test-pg.el
	cp proton-users.yaml /tmp
	${DOCKER} run --rm --name proton \
           --pull=newer \
           --publish 127.0.0.1:5113:5432 --publish 127.0.0.1:8123:8123 \
           -e TELEMETRY_ENABLED=false \
           -v /tmp/proton-users.yaml:/etc/proton-server/users.yaml \
           ghcr.io/timeplus-io/proton:latest
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@localhost:5113/default $(MAKE) test



# https://docs.greptime.com/getting-started/installation/greptimedb-standalone
#
# This database implements quite a lot of the PostgreSQL wire protocol, but the names it uses for
# types in the pg_catalog.pg_types table are not the same as PostgreSQL, so our parsing machinery
# does not work (we see 42 returned as "42").
#
# postgresql://pgeltestuser:pgeltest@localhost:4003/public
test-greptimedb: test-pg.el
	${DOCKER} run --rm --name greptimedb \
	  --pull=newer \
	  --publish 127.0.0.1:4003:4003 \
	  -d docker.io/greptime/greptimedb:latest standalone start \
	  --http-addr 0.0.0.0:4000 \
	  --rpc-addr 0.0.0.0:4001 \
	  --mysql-addr 0.0.0.0:4002 \
	  --postgres-addr 0.0.0.0:4003
	sleep 5
	PGEL_PORT=4003 PGEL_DATABASE=public $(MAKE) test
	${DOCKER} stop greptimedb


# RisingWave event streaming database.
# https://github.com/risingwavelabs/risingwave
# Also at ghcr.io/risingwavelabs/risingwave
# https://docs.risingwave.com/get-started/quickstart
#
# Fails on query "SELECT 'gday'::varchar(20)"
#
# Could test TLS support with environment variables RW_SSL_CERT and RW_SSL_KEY.
#
# The "root" user is set explicitly in risingwave/src/cmd_all/src/standalone.rs and there does not
# seem to be a mechanism to change it; likewise for the "dev" database name.
test-risingwave: test-pg.el
	${DOCKER} run --rm --name risingwave \
	  --pull=newer \
	  -e ENABLE_TELEMETRY=false \
	  --publish 127.0.0.1:4566:4566 \
	  -d docker.io/risingwavelabs/risingwave:latest single_node
	sleep 5
	PGURI=postgresql://root@127.0.0.1:4566/dev $(MAKE) test
	${DOCKER} stop risingwave


# Google AlloyDB Omni. This is PostgreSQL with proprietary Google-developed extensions, including a
# columnar storage extension, adaptive autovacuum, and an index advisor.
#
# https://cloud.google.com/alloydb/omni/docs/quickstart
# https://hub.docker.com/r/google/alloydbomni
# https://cloud.google.com/alloydb/docs/reference/database-flags
# Seems to ignore the PGPORT environment variable
test-alloydb: test-pg.el
	${DOCKER} run --rm --name alloydb \
          --pull=newer \
          --publish 127.0.0.1:4481:5432 \
          -e POSTGRES_DB=pgeltestdb \
          -e POSTGRES_USER=pgeltestuser \
          -e POSTGRES_PASSWORD=pgeltest \
          -d docker.io/google/alloydbomni:latest -c google_columnar_engine.enabled=on
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:4481/pgeltestdb $(MAKE) test
	${DOCKER} stop alloydb


# Doltgres provides database version control in Postgres-flavored SQL
# https://github.com/dolthub/doltgresql
# https://hub.docker.com/r/dolthub/doltgresql
test-doltgres: test-pg.el
	${DOCKER} run --rm --name doltgres \
           --publish 127.0.0.1:5489:5432 \
           -e DOLTGRES_USER=pgeltestuser \
           -e DOLTGRES_PASSWORD=pgeltest \
           -e DOLTGRES_DB=pgeltestdb \
           -d docker.io/dolthub/doltgresql:latest
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5489/pgeltestdb $(MAKE) test
	${DOCKER} stop doltgres


# CMU NoisePage, which seems to have been abandoned in 2023
# https://github.com/cmu-db/noisepage/blob/master/docs/tech_docker.md
# https://hub.docker.com/r/wwfalcon/noisepage
#
# This Docker image is not working.
test-noisepage: test-pg.el
	${DOCKER} run --rm --name noisepage \
          --net host \
          docker.io/wwfalcon/noisepage:latest
	sleep 10
	PGURI=postgresql://noisepage@127.0.0.1:15721/noisepage $(MAKE) test
	${DOCKER} stop noisepage



# https://seafowl.io/docs/guides/querying-postgresql
# https://hub.docker.com/r/splitgraph/seafowl/
# No longer under active development following aquisition by EDB
test-seafowl: test-pg.el
	${DOCKER} run --rm --name seafowl \
           --publish 127.0.0.1:6432 \
           -v "./seafowl.toml:/etc/seafowl.toml" \
           -d docker.io/splitgraph/seafowl:nightly -c /etc/seafowl.toml
	sleep 10
	PGURI=postgresql://seafowl@127.0.0.1:6432/pgeltestdb $(MAKE) test
	${DOCKER} stop seafowl


# Apache Datafusion with PostgreSQL protocol frontend
#
# https://github.com/datafusion-contrib/datafusion-postgres
#
# This database does not implement primay keys, nor cursors, nor CREATE INDEX, nor insert-returning,
# nor SQL types TIMETZ, BIT. BPCHAR.
test-datafusion: test-pg.el
	$(eval TMPBDIR := $(shell mktemp -d -t datafusion-buildXXXX))
	(cd ${TMPBDIR} && \
         git clone --depth 1 https://github.com/datafusion-contrib/datafusion-postgres && \
         cd datafusion-postgres && \
         cargo build --release)
	(cd ${TMPBDIR}/datafusion-postgres && cargo run --release -- -p 5666 & )
	sleep 5
	PGURI=postgresql://postgres@127.0.0.1:5666/pgeltestdb $(MAKE) test
	rm -rf ${TMPBDIR}


# https://hub.docker.com/r/clockworklabs/spacetime
# https://spacetimedb.com/docs/sql/pg-wire
# apparently also hosted on registry.digitalocean.com/clockwork/spacetimedb:latest
#
# Missing: user authentication
# https://spacetimedb.com/docs/http/identity
test-spacetimedb: test-pg.el
	${DOCKER} run --rm --name spacetimedb \
           --publish 127.0.0.1:5981:5981 \
           --publish 127.0.0.1:3000:3000 \
           -d docker.io/clockworklabs/spacetime:latest start --listen-addr 127.0.0.1:3000 --pg-port 5981
	sleep 2
	podman exec spacetimedb spacetime init --lang rust my_module
	podman exec spacetimedb spacetime publish --yes --project-path my_module pgeltestdb
#        @json=$(shell curl -X POST http://localhost:3000/v1/identity);
	curl http://localhost:3000/v1/ping  # check whether we can connect
	curl -X POST http://localhost:3000/v1/identity
	sleep 1
	PGURI=postgresql://pgeltestuser:foo@127.0.0.1:5981/pgeltestdb $(MAKE) test
	${DOCKER} stop spacetimedb


# https://hub.docker.com/r/ravendb/ravendb/
# https://docs.ravendb.net/7.1/integrations/postgresql-protocol/overview/#enabling-postgresql-support
#
# It seems that the PostgreSQL protocol support requires a licence, so this doesn't work.
test-ravendb: test-pg.el
	${DOCKER} run --rm --name ravendb \
          -e RAVEN_Setup_Mode=None \
          -e RAVEN_ARGS="--log-to-console" \
          -e Features_Availability=Experimental \
          -e Integrations_PostgreSQL_Enabled=true \
          -e Integrations_PostgreSQL_Port=5433 \
          --publish 127.0.0.1:5433:5433 \
          docker.io/ravendb/ravendb-nightly:ubuntu-latest
	sleep 5
	PGURI=postgresql://ravendb@127.0.0.1:5433/ravendb $(MAKE) test
	${DOCKER} stop ravendb


# https://docs.mindsdb.com/setup/self-hosted/docker
#
# As of 2025-09 the PostgreSQL wire protocol support does not seem fully working. We can connect to
# the database, but any query triggers an error "AttributeError: 'list' object has no attribute
# 'columns'".
test-mindsdb: test-pg.el
	${DOCKER} run --rm --name mindsdb \
          --net host \
          -e MINDSDB_USERNAME=pgeltestuser \
          -e MINDSDB_PASSWORD=pgeltest \
          -e MINDSDB_APIS=postgres \
          -e MINDSDB_NO_STUDIO=true \
          -d docker.io/mindsdb/mindsdb:latest
	sleep 10
	echo 'CREATE DATABASE pgeltestdb WITH ENGINE = "postgres", PARAMETERS = { "user": "pgeltestuser", "password": "pgeltest", "host": "localhost", "port": "5432", "database": "pgeltestdb" };' | psql postgresql://pgeltestuser:pgeltest@127.0.0.1:55432/pgeltestdb
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:55432/pgeltestdb $(MAKE) test
	${DOCKER} stop mindsdb


# YottaDB Octo
#
# https://gitlab.com/YottaDB/DBMS/YDBOcto/-/blob/master/README.md#using-the-docker-images
# https://docs.yottadb.com/Octo/config.html
#
# To give a user permission to modify schemas, recreate the user using ydboctoAdmin with the --allowschemachanges
#   yottadb -r %ydboctoAdmin add user OctoUser --readwrite --allowschemachanges
#
# also at docker.io/yottadb/octo:latest
test-octodb: test-pg.el
	cp octo-entrypoint.sh /tmp
	chmod og+x /tmp/octo-entrypoint.sh
	${DOCKER} run --rm --name octodb \
          --pull=newer \
          --publish 127.0.0.1:5669:1337 \
          -v /tmp/octo-entrypoint.sh:/entrypoint.sh \
          -d docker.io/yottadb/octo-vehu:latest-master
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5669/hello $(MAKE) test
	${DOCKER} stop octodb


# https://hub.docker.com/r/opentext/vertica-ce
#
# As of 2025-09 it seems that OpenText have deleted all references to their community edition from
# Dockerhub and from their website...
#
# https://github.com/vertica/vertica-containers/blob/main/one-node-ce/Dockerfile_Ubuntu
test-vertica: test-pg.el
	${DOCKER} run --rm --name vertica \
           --pull=newer \
           --publish 127.0.0.1:5433:5433 --publish 127.0.0.1:5444:5444 \
           -e VERTICA_DB_NAME=pgeltestdb \
           -e VERTICA_DB_USER=pgeltestuser \
           -e VERTICA_DB_PASSWORD=pgeltest \
           -d docker.io/opentext/vertica-ce:latest
	sleep 10
	PGURI=postgresql://dbadmin:pgeltest@127.0.0.1:5433/pgeltestdb $(MAKE) test
	${DOCKER} stop vertica


# https://docs.arcadedb.com/#postgres-driver
# https://github.com/ArcadeData/arcadedb
#
# Note: syntax compatibility with PostgreSQL is very low
# https://docs.arcadedb.com/#sql-syntax
# In particular, "CREATE TABLE" does not work, needs "CREATE TYPE"
test-arcadedb: test-pg.el
	${DOCKER} run --rm --name arcadedb \
          --pull=newer \
          --publish 127.0.0.1:5600:5432 \
          -e JAVA_OPTS="-Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin -Darcadedb.server.rootPassword=pgeltest -Darcadedb.server.defaultDatabases=pgeltestdb[pgeltestuser:pgeltest]" \
          -d docker.io/arcadedata/arcadedb:latest
	sleep 10
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5600/pgeltestdb $(MAKE) test
	${DOCKER} stop arcadedb


# https://stoolap.io/
# https://github.com/stoolap/stoolap
#
# We don't currently bother recognizing this as a PostgreSQL variant, because its PostgreSQL
# compatibility at the SQL level is very low. For instance, it doesn't recognize schema-qualified
# names such as schema.table.
test-stoolap-old: test-pg.el
	$(eval TMPBDIR := $(shell mktemp -d -t stoolap-buildXXXX))
	(cd ${TMPBDIR} && \
         git clone --depth 1 https://github.com/stoolap/stoolap.git && \
         cd stoolap && \
         go build -o /tmp/stoolap-pgserver ./cmd/stoolap-pgserver)
	/tmp/stoolap-pgserver --database memory:// --bind 127.0.0.1:5401 &
	sleep 2
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5401/stoolap $(MAKE) test
	rm -rf /tmp/stoolap-pgserver ${TMPBDIR}

# https://stoolap.io/
# https://github.com/stoolap/stoolap
#
# But this rewrite in Rust of the Go project doesn't currently support the PostgreSQL network protocol.
test-stoolap: test-pg.el
	$(eval TMPBDIR := $(shell mktemp -d -t stoolap-buildXXXX))
	(cd ${TMPBDIR} && \
         git clone --depth 1 https://github.com/stoolap/stoolap && \
         cd stoolap && \
         cargo build)
	(cd ${TMPBDIR}/stoolap && cargo run -- --db memory:// --bind 127.0.0.1:5401 &)
	sleep 2
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5401/stoolap $(MAKE) test
	rm -rf ${TMPBDIR}



# A Pgwire adaptor for GlueSQL. More an SQL engine than really providing PostgreSQL compatibility;
# basic functions like version() are not implemented.
#
# https://github.com/gluesql/gluesql
# https://github.com/sunng87/pgwire/blob/master/examples/gluesql.rs
test-gluesql: test-pg.el
	$(eval TMPBDIR := $(shell mktemp -d -t gluesql-buildXXXX))
	(cd ${TMPBDIR} && \
         git clone --depth 1 https://github.com/sunng87/pgwire/ && \
         cd pgwire && \
         cargo run --example gluesql &)
	sleep 2
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1/pgwire $(MAKE) test
	rm -rf ${TMPBDIR}



# https://www.kinetica.com/kinetica-developer-edition/
test-kinetica: test-pg.el
	${DOCKER} run --rm --name kinetica \
          --pull=newer \
          --publish 127.0.0.1:5434:5432 \
          -d docker.io/kinetica/kinetica-cpu:latest /bin/sh -c "/opt/gpudb/core/bin/gpudb start && sleep 1000"
	sleep 120   # fantastically slow to start up!
	PGURI=postgresql://admin:admin@127.0.0.1:5434/kinetica $(MAKE) test


# https://github.com/surrealdb/surrealdb?tab=readme-ov-file#run-using-docker
#
# It looks like PostgreSQL wire protocol support is "in development".
test-surrealdb: test-pg.el
	${DOCKER} run --rm --name surrealdb \
           --pull=newer \
           --publish 127.0.0.1:8000:8000 \
           -d docker.io/surrealdb/surrealdb:nightly start memory \
              --user pgeltestuser --pass pgeltest --log info --allow-experimental
	sleep 5
	PGURI=postgresql://pgeltestuser:pgeltest@127.0.0.1:5433/pgeltestdb $(MAKE) test
	${DOCKER} stop surrealdb


# TODO: test https://github.com/serenedb/serenedb ("distributed real-time search analytics DB")



# https://github.com/risinglightdb/risinglight
#
# This educational database implementation is not very PostgreSQL compatible; it doesn't implement
# version() for example.
test-risinglight: test-pg.el
	cd ~/tmp/risinglight && cargo run -- --server --port 5367 &
	sleep 2
	PGURI=postgresql://postgres@localhost:5367/postgres $(MAKE) test


# ChronDB -- git-backed database
# https://github.com/moclojer/chrondb
#
# This has extremely poor PostgreSQL wire protocol support (eg. no support for the extended query
# protocol).
test-chrondb: test-pg.el
	PGURI=postgresql://chrondb@127.0.0.1:5432/chrondb $(MAKE) test


# https://github.com/erans/pgsqlite -- a PostgreSQL protocol adapter for SQLite databases
#
# Last tested 2025-12 with v0.0.19
test-pgsqlite: test-pg.el
	$(eval TMPBDIR := $(shell mktemp -d -t pgsqlite-buildXXXX))
	(cd ${TMPBDIR} && \
         git clone --depth 1 https://github.com/erans/pgsqlite && \
         cd pgsqlite && \
         cargo build)
	(cd ${TMPBDIR}/pgsqlite && PGSQLITE_AUDIT_LOG_QUERIES=true cargo run --bin pgsqlite -- --port 7778 --in-memory & )
	sleep 2
	PGURI=postgresql://pgeltestuser@127.0.0.1:7778/pgeltestdb $(MAKE) test
	rm -rf ${TMPBDIR}


# Create and populate a new database with information concerning works by Shakespeare.
# Data from https://github.com/catherinedevlin/opensourceshakespeare
#
# We assume that our user pgeltestuser has already been set up.
setup-shakespeare:
	sudo -u postgres createdb --owner=pgeltestuser shakespeare
	curl -L https://raw.githubusercontent.com/catherinedevlin/opensourceshakespeare/master/shakespeare.sql \
	| grep -v "^ALTER.*OWNER TO postgres;" \
	| psql "host=localhost user=pgeltestuser dbname=shakespeare password=pgeltest"


# Pagila demo database (sample DVD rental database) from https://github.com/devrimgunduz/pagila
#
# The data is published in a form for import by user postgres, but we don't want to allow random SQL
# dumps on the internet to modify our database as the superuser. Therefore, we import as a user with
# reduced privileges and delete all SQL statements that refer to the postgres user and prevent
# import without privileges.
setup-pagila:
	sudo -u postgres createdb --owner=pgeltestuser pagila
	curl -L https://github.com/devrimgunduz/pagila/raw/refs/heads/master/pagila-schema.sql \
	| grep -v "^ALTER.*OWNER TO postgres;" \
	| psql "host=localhost user=pgeltestuser dbname=pagila password=pgeltest"
	curl -L https://github.com/devrimgunduz/pagila/raw/refs/heads/master/pagila-schema-jsonb.sql \
	| grep -v "^ALTER.*OWNER TO postgres;" \
	| psql "host=localhost user=pgeltestuser dbname=pagila password=pgeltest"
	curl -L https://raw.githubusercontent.com/devrimgunduz/pagila/refs/heads/master/pagila-data.sql \
	| grep -v "^ALTER.*OWNER TO postgres;" \
	| psql "host=localhost user=pgeltestuser dbname=pagila password=pgeltest"
	curl -L https://github.com/devrimgunduz/pagila/raw/refs/heads/master/pagila-insert-data_apt-jsonb.sql \
	| pg_restore -v --no-owner -d "host=localhost user=pgeltestuser dbname=pagila password=pgeltest"


# Digital media store, data from https://github.com/morenoh149/postgresDBSamples/tree/master/chinook-1.4
# The most commonly available source of this database includes corrupted UTF-8 data.
setup-chinook:
	sudo -u postgres createdb --owner=pgeltestuser chinook
	curl -L https://github.com/morenoh149/postgresDBSamples/raw/refs/heads/master/chinook-1.4/Chinook_PostgreSql_utf8.sql \
	| grep -v "^ALTER.*OWNER TO postgres;" \
	| psql "host=localhost user=pgeltestuser dbname=chinook password=pgeltest"


# Testing the asynchronous notification support implemented in v0.24 to run a publish-subcribe test.
# We run 4 separate Emacs instances, and one central PostgreSQL used as a "message broker" or "event
# bus". Note that CPU usage in this simple demo is very low.
pubsub: notification-subscriber.el notification-publisher.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l notification-subscriber.el -f do-listener &
	sleep 1
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l notification-publisher.el -f do-publisher


bench-uncompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-bench
	rm -rf ${DEPS_DIR}

bench-bytecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el --eval '(byte-compile-file "../pg.el")'
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el --eval "(load (native-compile \"../pg.el\"))" \
	   -l test-pg.el -f pg-bench

bench-nativecompiled-speed: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el --eval "(progn (setq native-comp-speed 3) (load (native-compile \"../pg.el\")))" \
	   -l test-pg.el -f pg-bench
