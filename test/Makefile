WORKDIR := $(shell mktemp -d)


test: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test

# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations (the PostgreSQL backend needs to be set up with TLS certificates), so this is a
# separate test target.
test-tls: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test-tls

# Run tests over a local Unix socket connection to PostgreSQL.
test-local: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test-local


# Using Docker images for Emacs from https://hub.docker.com/r/silex/emacs/ and the locally installed
# PostgreSQL.
test-emacs27: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	podman run -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   silex/emacs:27.2 \
	   emacs -Q --batch -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

test-emacs26: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	podman run -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   silex/emacs:26.3-alpine-ci \
	   emacs -Q --batch -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

# Emacs versions older than 26.1 will not work against a recent PostgreSQL version (that is set up
# to require SCRAM-SHA-256 authentication), because they don't include the GnuTLS support which we
# use to calculate HMACs. They may however work against a database set up to not require
# authentication for local connections.
test-emacs25: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	podman run -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   silex/emacs:25.3 \
	   emacs -Q --batch -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test

test-postgresql14: test-pg.el
	podman run --name pgsql \
	   --publish 5439:5439 \
	   -e POSTGRES_DB=pgeltestdb \
	   -e POSTGRES_USER=pgeltestuser \
	   -e POSTGRES_PASSWORD=pgeltest \
	   -e PGPORT=5439 \
	   -d docker.io/library/postgres:14
	sleep 5
	PGEL_PORT=5439 emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test
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
	PGEL_PORT=5439 emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test
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
	PGEL_PORT=5439 emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test
	podman stop pgsql
	podman rm pgsql


# CrateDB uses a default database name of "doc" that we can't set via Docker
test-cratedb: test-pg.el
	PGEL_DATABASE=doc make test

test-cockroachdb: test-pg.el
	podman run --name cockroachdb \
	   -v /dev/log:/dev/log \
	   --publish 26257:26257 \
	   -d cockroachdb/cockroach start-single-node --insecure
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=root PGEL_PASSWORD="" PGEL_PORT=26257 make test
	podman stop cockroachdb
	podman rm cockroachdb


test-questdb: test-pg.el
	podman run --name questdb \
	  --publish 8812:8812 \
	  -d questdb/questdb
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=admin PGEL_PASSWORD="quest" PGEL_PORT=8812 make test
	podman stop questdb
	podman rm questdb


test-yutabyte: test-pg.el
	podman run --name yugabyte \
	  --publish 5433:5433 \
	  -d yugabytedb/yugabyte \
	  bin/yugabyted start \
	  --base_dir=/tmp \
	  --daemon=false
	sleep 5
	PGEL_DATABASE=yugabyte PGEL_USER=yugabyte PGEL_PASSWORD="" PGEL_PORT=5433 make test
	podman stop yugabyte
	podman rm yugabyte

test-greenplum: test-pg.el
	podman run --name greenplum \
	  --publish 5433:5433 \
	  -e PGPORT=5433 \
	  -d docker.io/projectairws/greenplum:latest
	sleep 5
	PGEL_DATABASE=postgres PGEL_USER=gpadmin PGEL_PASSWORD="greenplum" PGEL_PORT=5432 make test
	podman stop greenplum
	podman rm greenplum


bench-uncompiled: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-bench

bench-bytecompiled: test-pg.el
	emacs -Q --batch --eval '(byte-compile-file "../pg.el")' -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el
	emacs -Q --batch --eval '(load (native-compile "../pg.el"))' -l test-pg.el -f pg-bench
