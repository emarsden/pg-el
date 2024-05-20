EMACS ?= emacs
WORKDIR := $(shell mktemp -d)
PEG := $(shell mktemp --tmpdir --suffix .el pgelpeg-XXXX)
PEGURL = "https://git.savannah.gnu.org/cgit/emacs/elpa.git/plain/peg.el?h=externals/peg"

default: test

# We can't use the package management machinery when starting Emacs with "-Q", so download
# and load our peg dependency explicitly.
.PHONY: peg
peg:
	wget -q -O ${PEG} ${PEGURL}

test: test-pg.el peg
	${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-test

# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations (the PostgreSQL backend needs to be set up with TLS certificates), so this is a
# separate test target.
test-tls: test-pg.el peg
	${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-test-tls

# Run tests over a local Unix socket connection to PostgreSQL.
test-local: test-pg.el peg
	${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-test-local

test-connections: test-pg.el peg
	${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-connection-tests


# Using Docker images for Emacs from https://hub.docker.com/r/silex/emacs/ and the locally installed
# PostgreSQL.
test-emacs28: test-pg.el peg
	cp ../pg.el test-pg.el ${WORKDIR}
	cp ${PEG} ${WORKDIR}/peg.el
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:28.1 \
	   ${EMACS} -Q --batch -l /tmp/peg.el -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# The peg.el library doesn't actually compile with Emacs 27 (void-function byte-run--set-speed), so
# this doesn't work.
test-emacs27: test-pg.el peg
	cp ../pg.el test-pg.el ${WORKDIR}
	cp ${PEG} ${WORKDIR}/peg.el
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:27.2 \
	   ${EMACS} -Q --batch -l /tmp/peg.el --eval '(byte-compile-file "/tmp/peg.el")' && \
	   ${EMACS} -Q --batch -l /tmp/peg.elc -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# The extended query support will not work in Emacs versions before 28.1, because functionality
# provided by the bindat libary is needed. However, other functionality based on the simple query
# protocol (the pg-exec function) should work fine.
test-emacs26: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:26.3-alpine-ci \
	   ${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-test

# Emacs versions older than 26.1 will not work against a recent PostgreSQL version (that is set up
# to require SCRAM-SHA-256 authentication), because they don't include the GnuTLS support which we
# use to calculate HMACs. They may however work against a database set up to not require
# authentication for local connections.
test-emacs25: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	podman run --rm -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   docker.io/silex/emacs:25.3 \
	   ${EMACS} -Q --batch -l ${PEG} -l ../pg.el -l test-pg.el -f pg-test

test-postgresql14: test-pg.el
	podman run --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:14
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql
	podman rm pgsql

test-postgresql13: test-pg.el
	podman run --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:13-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql
	podman rm pgsql

test-postgresql12: test-pg.el
	podman run --detach \
	   --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   docker.io/library/postgres:12-alpine
	sleep 5
	PGEL_PORT=5439 $(MAKE) test
	podman stop pgsql
	podman rm pgsql

test-postgresql11: test-pg.el
	podman run --detach \
	   --name pgsql \
	   --publish 5437:5437 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5437 \
	   docker.io/library/postgres:11-alpine
	sleep 5
	PGEL_PORT=5437 $(MAKE) test
	podman stop pgsql
	podman rm pgsql


# Supabase (https://supabase.com) provide hosted PostgreSQL instances with convenient web
# dashboards, management APIs and integrations with authentication libraries. They have a free tier.
# As of 2023-08, they are running PostgreSQL 15.1 on Aarch64.
test-supabase: test-pg.el peg
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=db.foobles.supabase.co PGEL_USER=postgres PGEL_DATABASE=postgres $(MAKE) test

test-supabase-tls: test-pg.el peg
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=db.foobles.supabase.co PGEL_USER=postgres PGEL_DATABASE=postgres $(MAKE) test-tls


# Neon (https://neon.tech/) provide hosted "serverless" PostgreSQL instances, which allow convenient
# automated scalability according to load. They have a free tier. As of 2023-11, they are running
# PostgreSQL 15.4 on AMD64.
test-neon: test-pg.el peg
	PGEL_PASSWORD=foobles PGEL_HOSTNAME=foobles.eu-central-1.aws.neon.tech PGEL_USER=username PGEL_DATABASE=main $(MAKE) test-tls



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

# Version 23.1.13 of CockroachDB doesn't get very far through the tests, failing on the boolean
# vector syntax b'1001000'.
test-cockroachdb: test-pg.el
	podman run --rm --name cockroachdb \
	   -v /dev/log:/dev/log \
	   --publish 26257:26257 \
	   -d docker.io/cockroachdb/cockroach start-single-node --insecure
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=root PGEL_PASSWORD="" PGEL_PORT=26257 $(MAKE) test
	podman stop cockroachdb
	podman rm cockroachdb


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
	PGEL_DATABASE=postgres PGEL_USER=admin PGEL_PASSWORD="quest" PGEL_PORT=8812 $(MAKE) test
	podman stop questdb
	podman rm questdb


# Yugabyte 2.19 is based on PostgreSQL 11.2. This works very well, including the HSTORE and pgvector
# extensions. LISTEN/NOTIFY is not supported.
test-yugabyte: test-pg.el
	podman run --rm --name yugabyte \
          --publish 7000:7000 \
          --publish 9000:9000 \
          --publish 15433:15433 \
	  --publish 5433:5433 \
	  -d docker.io/yugabytedb/yugabyte \
	  bin/yugabyted start \
	  --base_dir=/tmp \
	  --background=false
	sleep 10
	PGEL_DATABASE=yugabyte PGEL_USER=yugabyte PGEL_PASSWORD="" PGEL_PORT=5433 $(MAKE) test
	podman stop yugabyte
	podman rm yugabyte



# 20230807 this container fails to run with podman with a permission denied on su
test-greenplum: test-pg.el
	podman run --rm --name greenplum \
	  --publish 5433:5433 \
	  -e PGPORT=5433 \
	  -d docker.io/projectairws/greenplum:latest
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=gpadmin PGEL_PASSWORD="greenplum" PGEL_PORT=5433 $(MAKE) test
	podman stop greenplum
	podman rm greenplum


# https://docs.paradedb.com/introduction#get-started
#
# As of 2024-05, the paradedb docker image is built on a Debian PostgreSQL 16.2.
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
	podman rm paradedb


# Testing the asynchronous notification support implemented in v0.24 to run a publish-subcribe test.
# We run 4 separate Emacs instances, and one central PostgreSQL used as a "message broker" or "event
# bus". Note that CPU usage in this simple demo is very low.
pubsub: notification-subscriber.el notification-publisher.el
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	${EMACS} -Q --batch -l ../pg.el -l notification-subscriber.el -f do-listener &
	sleep 1
	${EMACS} -Q --batch -l ../pg.el -l notification-publisher.el -f do-publisher


bench-uncompiled: test-pg.el
	${EMACS} -Q --batch -l ../pg.el -l test-pg.el -f pg-bench

bench-bytecompiled: test-pg.el
	${EMACS} -Q --batch --eval '(byte-compile-file "../pg.el")' -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el
	${EMACS} -Q --batch --eval '(load (native-compile "../pg.el"))' -l test-pg.el -f pg-bench
