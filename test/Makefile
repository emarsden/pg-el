WORKDIR := $(shell mktemp -d)


test: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test

# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations, so this is a separate test target.
test-tls: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test-tls


# Using Docker images for emacs from https://hub.docker.com/r/silex/emacs/
#
# This is failing because we don't have nettle-pbkdf2 installed in the container. 
test-emacs26: test-pg.el
	cp ../pg.el test-pg.el ${WORKDIR}
	sudo podman run -it \
	   -v ${WORKDIR}:/tmp \
	   --network slirp4netns:allow_host_loopback=true -e PGEL_HOSTNAME=10.0.2.2 \
	   silex/emacs:26.3-alpine-ci \
	   emacs -Q --batch -l /tmp/pg.el -l /tmp/test-pg.el -f pg-test


bench-uncompiled: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-bench

bench-bytecompiled: test-pg.el
	emacs -Q --batch --eval '(byte-compile-file "../pg.el")' -l ../pg.elc -l test-pg.el -f pg-bench

bench-nativecompiled: test-pg.el
	emacs -Q --batch --eval '(native-compile "../pg.el" "/tmp/pgelnative.eln")' -l /tmp/pgelnative.eln -l test-pg.el -f pg-bench
