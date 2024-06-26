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
	${EMACS} -Q --batch -L .. -L . -l load-deps.el ../pg.el -l test-pg.el -f pg-test

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
test-emacs28: test-pg.el
	cp install-deps.el load-deps.el ../pg.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:28.1 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# The peg.el library doesn't actually compile with Emacs 27 (void-function byte-run--set-speed), so
# this doesn't work.
test-emacs27: test-pg.el
	cp install-deps.el load-deps.el ../pg.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   -e DEPS_DIR=/tmp/deps \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
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
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
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
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:25.3 \
	   ${EMACS} -Q --batch /tmp/pg.el -l /tmp/install-deps.el -l /tmp/load-deps.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

test-postgresql17b: test-pg.el
	podman run --rm --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:17beta1-alpine3.20
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
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
	   docker.io/library/postgres:12-alpine
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
	   docker.io/library/postgres:11-alpine
	sleep 5
	PGEL_PORT=5437 $(MAKE) test
	podman stop pgsql


# Supabase (https://supabase.com) provide hosted PostgreSQL instances with convenient web
# dashboards, management APIs and integrations with authentication libraries. They have a free tier.
# As of 2023-08, they are running PostgreSQL 15.1 on Aarch64.
test-supabase: test-pg.el
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=db.foobles.supabase.co PGEL_USER=postgres PGEL_DATABASE=postgres $(MAKE) test

test-supabase-tls: test-pg.el
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=db.foobles.supabase.co PGEL_USER=postgres PGEL_DATABASE=postgres $(MAKE) test-tls


# Neon (https://neon.tech/) provide hosted "serverless" PostgreSQL instances, which allow convenient
# automated scalability according to load. They have a free tier. As of 2023-11, they are running
# PostgreSQL 15.4 on AMD64.
test-neon: test-pg.el
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=foobles.eu-central-1.aws.neon.tech PGEL_USER=username PGEL_DATABASE=main $(MAKE) test-tls



# https://docs.timescale.com/self-hosted/latest/install/installation-docker/
test-timescale: test-pg.el
	podman run --rm --name timescale \
	   --publish 5981:5981 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5981 \
	   docker.io/timescale/timescaledb-ha:pg16
	sleep 5
	PGEL_PORT=5981 $(MAKE) test-pgvector
	podman stop timescale


# CrateDB uses a default database name of "doc" that we can't set via Docker. Last tested with
# cratedb v5.7.1. Doesn't get very far through the tests: CrateDB doesn't accept a query which only
# contains an SQL comment.
#
# https://hub.docker.com/_/crate/
test-cratedb: test-pg.el
	podman run --rm --name cratedb \
          --publish=4200:4200 \
          --publish=5789:5789 \
          -e CRATE_HEAP_SIZE=1g \
          -d docker.io/crate -Cdiscovery.type=single-node -Cpsql.port=5789
	sleep 5
	PGEL_DATABASE=doc PGEL_USER=crate PGEL_DATABASE=postgres PGEL_PORT=5789 $(MAKE) test
	podman stop cratedb

# Version 23.2 of CockroachDB doesn't get very far through the tests, with an internal error
# generated by our query for pg-table-owner, and failing on the boolean vector syntax b'1001000'.
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
# extensions. The sequence test fails (SELECT last_value FROM pg_sequences). LISTEN/NOTIFY is not supported.
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
	expect -c 'spawn podman exec -ti immudb immuadmin user create pgeltestuser readwrite pgeltestdb;sleep 0.1;expect "Choose a password for";send "pgeltest\r";expect "continue with your password instead.*Y/n";send "Y\r";expect "Confirm password";send "pgeltest\r";exit'
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
# As of 2024-06, the paradedb docker image is built on a Debian PostgreSQL 16.3.
test-paradedb: test-pg.el
	podman run --rm --name paradedb \
	  --publish 5441:5441 \
          -e POSTGRES_DATABASE=pgeltestdb \
          -e POSTGRESQL_PORT_NUMBER=5441 \
          -e PGPORT=5441 \
          -e POSTGRESQL_MASTER_PORT_NUMBER=5441 \
          -e POSTGRES_USERNAME=pgeltestuser \
          -e POSTGRESQL_PASSWORD=pgeltest \
          -e POSTGRESQL_POSTGRES_PASSWORD=pgeltest \
          -d docker.io/paradedb/paradedb:latest
	sleep 5
	PGEL_PORT=5441 $(MAKE) test
	podman stop paradedb


# Tested 2024-06 with v24.5. Very limited PostgreSQL support: there is no pg_type table so
# we can't retrieve information regarding the OID of builtin types.
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
	${EMACS} -Q --batch -l load-deps.el -l ../pg.el -l test-pg.el -f pg-bench
	rm -rf ${DEPS_DIR}

bench-bytecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -l load-deps.el --eval '(byte-compile-file "../pg.el")'
	${EMACS} -Q --batch -l load-deps.el -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el install-deps
	${EMACS} -Q --batch -l load-deps.el --eval "(load (native-compile \"../pg.el\"))" \
	   -l test-pg.el -f pg-bench
