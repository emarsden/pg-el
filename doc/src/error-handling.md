# Error handling

Errors signaled by PostgreSQL will be converted into an Emacs Lisp error that subclasses `pg-error`.
You can handle these errors as usual in Emacs Lisp, as shown in the example below. 

~~~admonish example title="Basic error handling"
```lisp
ELISP> (ignore-errors (pg-exec *pg* "SELECT ###"))
nil
ELISP> (condition-case nil
           (pg-exec *pg* "SELECT ###")
         (pg-error 42))
42 (#o52, #x2a, ?*)
```
~~~

Some errors are converted into specific subclasses of `pg-error`, as listed below. We can
discriminate the error category thanks to [PostgreSQL's SQLSTATE support](See
https://www.postgresql.org/docs/17/errcodes-appendix.html).

| Error class                       | Meaning                                                                         |
|-----------------------------------|---------------------------------------------------------------------------------|
| pg-connection-error               | Connection failure                                                              |
| pg-invalid-password               | Invalid password or authentication data                                         |
| pg-feature-not-supported          | PostgreSQL feature not supported                                                |
| pg-syntax-error                   | Syntax error                                                                    |
| pg-undefined-table                | Undefined table                                                                 |
| pg-undefined-column               | Undefined column                                                                |
| pg-undefined-function             | Undefined function                                                              |
| pg-copy-failed                    | PostgreSQL COPY failed                                                          |
| pg-connect-timeout                | PostgreSQL connection attempt timed out                                         |
| pg-type-error                     | When serializing, an argument was of an unexpected type                         |
| pg-numeric-value-out-of-range     | Numeric value out of range                                                      |
| pg-division-by-zero               | Division by zero                                                                |
| pg-floating-point-exception       | Floating point exception                                                        |
| pg-array-subscript-error          | Array subscript error                                                           |
| pg-datetime-field-overflow        | Overflow in a datetime field                                                    |
| pg-invalid-text-representation    | Invalid text representation                                                     |
| pg-invalid-binary-representation  | Invalid binary representation                                                   |
| pg-character-not-in-repertoire    | Character not in repertoire                                                     |
| pg-datatype-mismatch              | Datatype mismatch                                                               |
| pg-json-error                     | JSON-related error                                                              |
| pg-integrity-constraint-violation | Violation of an integrity constraint                                            |
| pg-restrict-violation             | Restrict violation                                                              |
| pg-not-null-violation             | Violation of a not NULL constraint                                              |
| pg-foreign-key-violation          | Violation of a FOREIGN KEY constraint                                           |
| pg-unique-violation               | Violation of a UNIQUE constraint                                                |
| pg-check-violation                | Violation of a CHECK constraint                                                 |
| pg-exclusion-violation            | Violation of an exclusion constraint                                            |
| pg-plpgsql-error                  | PL/pgSQL error                                                                  |
| pg-transaction-timeout            | Transaction timeout                                                             |
| pg-insufficient-resources         | Insufficient resources on the backend server (eg. memory full)                  |
| pg-disk-full                      | Disk full on the backend server                                                 |
| pg-too-many-connections           | Too many connections to the backend                                             |
| pg-internal-error                 | Internal error in the backend                                                   |



You can undertake error handling for specific error categories as shown in the example below:

~~~admonish example title="Differentiated error handling"
```lisp
ELISP> (condition-case nil
           (pg-exec *pg* "SELECT 2147483649::int4")
         (pg-numeric-value-out-of-range (message "Numeric overflow"))
	 (pg-syntax-error (message "Syntax error"))
	 (pg-error (message "Generic error")))
"Numeric overflow"
```
~~~


Please note that some semi-compatible PostgreSQL variants do not implement fine-grained SQLSTATE
error reporting, simply returning most errors as an “internal error” (this is the case of CrateDB in
2025-02, for example).
