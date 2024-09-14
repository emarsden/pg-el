EMACS ?= emacs
WORKDIR := $(shell mktemp -d)

# export DEPS_DIR = $(shell realpath .deps)
export DEPS_DIR := $(shell mktemp -d --tmpdir emacsdepsXXXX)

default: test

# Note: we have to include test/ in test/install-deps.el because when we load ../pg.el we change the
# current directory to the parent of test.
.PHONY: install-deps
install-deps:
	${EMACS} -Q --batch ../pg.el -l test/install-deps.el

test: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el ../pg.el \
	   -l pg.el -l pg-geometry.el -l pg-gis.el -l test-pg.el -f pg-test

test-interactive: test-pg.el install-deps
	${EMACS} -Q -L .. -L . -l load-deps.el ../pg.el \
	   -l pg.el -l pg-geometry.el -l pg-gis.el -l test-pg.el -f pg-test

supa: install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l supabase.el -f test

xata: install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l xata-bug.el -f test


# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations (the PostgreSQL backend needs to be set up with TLS certificates), so this is a
# separate test target.
test-tls: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-tls

# Run tests over a local Unix socket connection to PostgreSQL.
test-local: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-local

test-connections: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-connection-tests

test-pgvector: test-pgvector.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pgvector.el -f pg-test


# Using Docker images for Emacs from https://hub.docker.com/r/silex/emacs/ and the locally installed
# PostgreSQL.
test-emacs-dev: test-pg.el
	cp install-deps.el load-deps.el ../pg.el ../pg-geometry.el ../pg-gis.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:master-alpine-ci \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l ../tmp/pg-geometry.el -l /tmp/pg-gis.el -l /tmp/test-pg.el -f pg-test

test-emacs28: test-pg.el
	cp install-deps.el load-deps.el ../pg.el ../pg-geometry.el ../pg-gis.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:28.1 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/pg-geometry.el -l /tmp/pg-gis.el -l /tmp/test-pg.el -f pg-test

# The peg.el library doesn't actually compile with Emacs 27 (void-function byte-run--set-speed), so
# this doesn't work.
test-emacs27: test-pg.el
	cp install-deps.el load-deps.el ../pg.el test-pg.el ${WORKDIR}
	podman run --rm -it \
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
	podman run --rm -it \
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
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   -e PGEL_HOSTNAME=10.0.2.2 \
	   --network slirp4netns:allow_host_loopback=true \
	   docker.io/silex/emacs:25.3 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

test-postgresql17: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5426:5426 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5426 \
	   -d docker.io/library/postgres:17rc1-alpine3.20
	sleep 5
	PGEL_PORT=5426 $(MAKE) test
	podman stop pgsql

test-postgresql16: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5416:5416 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5416 \
	   -d docker.io/library/postgres:16.3-bullseye
	sleep 5
	PGEL_PORT=5416 $(MAKE) test
	podman stop pgsql

test-postgresql14: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:14
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql

test-postgresql13: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:13-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql

test-postgresql12: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:12-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql

test-postgresql11: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5437:5437 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5437 \
	   -d docker.io/library/postgres:11-alpine
	sleep 5
	PGEL_PORT=5437 $(MAKE) test
	podman stop pgsql


# EDB with a Red Hat Universal Base Image. This defaults to an SQL_ASCII client-encoding unless we
# set LC_CTYPE.
#
# https://github.com/EnterpriseDB/docker-postgresql
test-enterprisedb: test-pg.el
	podman run --rm --name edb \
	   --publish 5366:5366 \
	   -e LANG=en_US.UTF8 \
	   -e LC_CTYPE=en_US.UTF8 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5366 \
	   -d ghcr.io/enterprisedb/postgresql:16
	PGEL_PORT=5366 $(MAKE) test
	podman stop edb


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
	podman volume create pgcerts
	tar cf ${WORKDIR}/certs.tar --directory=${WORKDIR} server.crt server.key root.crt
	podman volume import pgcerts ${WORKDIR}/certs.tar
	podman run --rm --name pgsqltls \
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
	   -d docker.io/bitnami/postgresql:latest
	sleep 5
	PGEL_CLIENT_CERT=${WORKDIR}/client.crt PGEL_CLIENT_CERT_KEY=${WORKDIR}/client.key PGEL_PORT=5488 ${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-test-client-cert
	podman stop pgsqltls
	sleep 2
	podman volume rm pgcerts


# Supabase (https://supabase.com) provide hosted PostgreSQL instances with convenient web
# dashboards, management APIs and integrations with authentication libraries. They have a free tier.
# As of 2023-08, they are running PostgreSQL 15.1 on Aarch64. 
test-supabase: test-pg.el
	$(MAKE) test-tls

# Neon (https://neon.tech/) provide hosted "serverless" PostgreSQL instances, which allow convenient
# automated scalability according to load. They have a free tier. As of 2023-11, they are running
# PostgreSQL 15.4 on AMD64.
test-neon: test-pg.el
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=foobles.eu-central-1.aws.neon.tech PGEL_USER=username PGEL_DATABASE=main $(MAKE) test-tls


# app.xata.io are running PostgreSQL 15.5 on aarch64/Linux as of 2024-07
test-xata: test-pg.el
	PGURI="postgresql://user:password@eu-central-1.sql.xata.sh/pgeltestdb:main?sslmode=force" $(MAKE) test-tls


# https://docs.timescale.com/self-hosted/latest/install/installation-docker/
test-timescale: test-pg.el
	podman run --rm --name timescale \
	   --publish 5981:5981 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5981 \
	   -d docker.io/timescale/timescaledb-ha:pg16
	sleep 5
	PGEL_PORT=5981 $(MAKE) test
	PGEL_PORT=5981 $(MAKE) test-pgvector
	podman stop timescale


# CrateDB uses a default database name of "doc" that we can't set via Docker. Last tested with
# cratedb v5.8.2. Doesn't get very far through the tests: CrateDB doesn't accept a query which only
# contains an SQL comment.
#
# https://hub.docker.com/_/crate/
test-cratedb: test-pg.el
	podman run --rm --name cratedb \
          --publish 4200:4200 \
          --publish 5789:5789 \
          -e CRATE_HEAP_SIZE=1g \
          -d docker.io/crate -Cdiscovery.type=single-node -Cpsql.port=5789
	sleep 5
	PGEL_USER=crate PGEL_DATABASE=postgres PGEL_PORT=5789 $(MAKE) test
	podman stop cratedb

# In 2024-09 version 24.2 of CockroachDB doesn't get very far through the tests, with an internal
# error generated by our query for pg-table-owner, and failing on the boolean vector syntax
# b'1001000'.
test-cockroachdb: test-pg.el
	podman run --rm --name cockroachdb \
	   -v /dev/log:/dev/log \
	   --publish 26257:26257 \
	   -d docker.io/cockroachdb/cockroach start-single-node --insecure
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=root PGEL_PASSWORD="" PGEL_PORT=26257 $(MAKE) test
	podman stop cockroachdb


# The current version of QuestDB (based on PostgreSQL 12.3) doesn't get very far through the tests;
# it fails on "SELECT 1::integer" because it doesn't recognize integer as a type. Also DELETE
# statements are not supported.
#
# https://questdb.io/docs/configuration/
test-questdb: test-pg.el
	podman run --rm --name questdb \
          --publish 8812:8812 \
          --publish 9000:9000 \
          -e QDB_PG_USER=pgeltestuser \
          -e QDB_PG_PASSWORD=pgeltest \
	  -d docker.io/questdb/questdb
	sleep 5
	PGEL_HOSTNAME=127.0.0.1 PGEL_DATABASE=postgres PGEL_USER=pgeltestuser PGEL_PASSWORD=pgeltest PGEL_PORT=8812 $(MAKE) test
	podman stop questdb


# Yugabyte 2.21 is based on PostgreSQL 11.2. This works very well, including the HSTORE and pgvector
# extensions. The sequence test fails (SELECT last_value FROM pg_sequences). Some more recent SQL
# evolutions suchas "INTEGER GENERATED ALWAYS AS expression STORED" are not supported. LISTEN/NOTIFY
# is not supported.
test-yugabyte: test-pg.el
	podman run --rm --name yugabyte \
          --publish 7000:7000 \
          --publish 9000:9000 \
          --publish 15433:15433 \
	  --publish 5433:5433 \
	  -d docker.io/yugabytedb/yugabyte \
	  bin/yugabyted start \
	  --advertise_address localhost \
	  --base_dir=/tmp \
	  --background=false
	sleep 10
	PGEL_DATABASE=yugabyte PGEL_USER=yugabyte PGEL_PASSWORD="" PGEL_PORT=5433 $(MAKE) test
	podman stop yugabyte


# https://docs.immudb.io/master/running/download.html
#
# immadmin doesn't seem to provide a useful way to provide password via environment
# not to be used in a non-interactive manner
#
# We see a protocol error "database name not provided" after the StartupMessage
# whereas psql is able to connect when disabling TLS
#   psql "sslmode=disable host=localhost port=5667 dbname=pgeltestdb user=pgeltestuser password=pgeltest"
# though few SQL commands work.
test-immudb: test-pg.el
	podman run --rm --name immudb \
	  --net host \
          -d docker.io/codenotary/immudb:latest \
            --pgsql-server --pgsql-server-port 5667 \
	    --admin-password pgeltest --force-admin-password
	sleep 2
	echo pgeltest|podman exec -ti immudb immuadmin login immudb
	podman exec -ti immudb immuadmin database create pgeltestdb
	expect -c 'spawn podman exec -ti immudb immuadmin user create pgeltestuser readwrite pgeltestdb;sleep 0.1;expect "Choose a password for";sleep 0.1;send "pgeltest\r";expect "continue with your password instead.*Y/n";sleep 0.1;send "Y\r";expect "Confirm password";sleep 0.1;send "pgeltest\r";sleep 0.1;exit'
	PGEL_DATABASE=pgeltestdb PGEL_USER=pgeltestuser PGEL_PASSWORD=pgeltest PGEL_PORT=5667 $(MAKE) test
	podman stop immudb


# 20230807 this container fails to run in podman with a permission denied on su
test-greenplum: test-pg.el
	podman run --rm --name greenplum \
	  --publish 5433:5433 \
	  -e PGPORT=5433 \
	  -d docker.io/projectairws/greenplum:latest
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=gpadmin PGEL_PASSWORD="greenplum" PGEL_PORT=5433 $(MAKE) test
	podman stop greenplum


# An ElasticSearch alternative, built as a PostgreSQL extension.
# https://docs.paradedb.com/introduction#get-started
#
# As of 2024-08, the paradedb docker image is built on a Debian PostgreSQL 16.3.
test-paradedb: test-pg.el
	podman run --rm --name paradedb \
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
	podman stop paradedb


# Google Spanner emulator
# See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/emulator.md
# https://cloud.google.com/spanner/docs/pgadapter-start#docker
test-spanner: test-pg.el
	podman run --rm --name spanner \
	  --publish 5499:5432 \
	 -d gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator -d pgeltestdb
	sleep 2
	PGEL_PORT=5499 PGEL_DATABASE=pgeltestdb $(MAKE) test


# YDB by Yandex
# https://ydb.tech/docs/en/postgresql/docker-connect
test-ydb: test-pg.el
	podman run --rm --name ydb \
	  --publish 5411:5432 --publish 8765:8765 \
	  -e POSTGRES_USER=pgeltestuser \
	  -e POSTGRES_PASSWORD=pgeltest \
	  -e YDB_EXPERIMENTAL_PG=1 \
	  -e YDB_USE_IN_MEMORY_PDISKS=true \
	  -d ghcr.io/ydb-platform/local-ydb:nightly
	sleep 10
	PGEL_PORT=5411 $(MAKE) test
	podman stop ydb


# Tested 2024-08 with v24.6. Very limited PostgreSQL support: there is no pg_type table so we can't
# retrieve information regarding the OID of builtin types. During our initialization sequence we
# send the query "SET datestyle = 'ISO'", which fails and causes the network connection to be reset (!). 
test-clickhouse: test-pg.el
	echo '<clickhouse><postgresql_port>5491</postgresql_port><core_dump><size_limit>0</size_limit></core_dump></clickhouse>' > /tmp/pgel-config.xml
	podman run --rm --name clickhouse \
	   --ulimit nofile=62144:62144 --ulimit core=234567:234567 \
	   -v /tmp/pgel-config.xml:/etc/clickhouse-server/config.d/pgel-config.xml \
	   --publish 5491:5491 \
	   -p 18123:8123 -p 9000:9000 \
	   -e CLICKHOUSE_DB=pgeltestdb \
	   -e CLICKHOUSE_USER=pgeltestuser \
	   -e CLICKHOUSE_PASSWORD=pgeltest \
	   -d docker.io/clickhouse/clickhouse-server
	sleep 5
	PGEL_PORT=5491 $(MAKE) test
	podman stop clickhouse


# https://docs.greptime.com/getting-started/installation/greptimedb-standalone
#
# This database implements quite a lot of the PostgreSQL wire protocol, but the names it uses for
# types in the pg_catalog.pg_types table are not the sames as PostgreSQL, so our parsing machinery
# does not work.
test-greptimedb: test-pg.el
	podman run --rm --name greptimedb \
	  --publish 127.0.0.1:4003:4003 \
	  -d docker.io/greptime/greptimedb:latest standalone start \
	  --http-addr 0.0.0.0:4000 \
	  --rpc-addr 0.0.0.0:4001 \
	  --mysql-addr 0.0.0.0:4002 \
	  --postgres-addr 0.0.0.0:4003
	sleep 5
	PGEL_PORT=4003 PGEL_DATABASE=public $(MAKE) test
	podman stop greptimedb


# Testing the asynchronous notification support implemented in v0.24 to run a publish-subcribe test.
# We run 4 separate Emacs instances, and one central PostgreSQL used as a "message broker" or "event
# bus". Note that CPU usage in this simple demo is very low.
pubsub: notification-subscriber.el notification-publisher.el
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	sleep 1
	${EMACS} -Q --batch -l ../pg.el -l notification-publisher.el -f do-publisher


bench-uncompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.el -l test-pg.el -f pg-bench
	rm -rf ${DEPS_DIR}

bench-bytecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el --eval '(byte-compile-file "../pg.el")'
	${EMACS} -Q --batch -L .. -L . -l load-deps.el -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -L .. -L . -l load-deps.el --eval "(load (native-compile \"../pg.el\"))" \
	   -l test-pg.el -f pg-bench
