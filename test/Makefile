test: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test

# Run tests over an encrypted connection to PostgreSQL. Encryption is not available for all versions
# and installations, so this is a separate test target.
test-tls: test-pg.el
	emacs -Q --batch -l ../pg.el -l test-pg.el -f pg-test-tls
