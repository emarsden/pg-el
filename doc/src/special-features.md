# Special pg-el features 


## Handling parameter changes

The PostgreSQL backend informs connected clients when certain server parameters change, by sending
them a special `ParameterStatus` message. These notifications are sent for `GUC_REPORT` parameters,
which include the `client_encoding`, the `DateStyle`, `TimeZone`, `server_encoding`,
`in_hot_standby` and `is_superuser`. You can register your interest in these messages by adding a
handler function to `pg-parameter-change-functions`. Each of these handler functions will be called
when such a message is received, with three arguments: the connection to PostgreSQL, the parameter
name and the parameter value.

These messages are sent asynchronously. 


~~~admonish example title="Handling changes to session timezone"
```lisp
ELISP> (defun handle-tz-change (_con name value)
         (when (string= name "TimeZone")
           (message "New timezone in PostgreSQL is %s" value)))
handle-tz-change
ELISP> (cl-pushnew #'handle-tz-change pg-parameter-change-functions)
(handle-tz-change pg-handle-parameter-client-encoding)
ELISP> (pg-result (pg-exec *pg* "SET SESSION TIME ZONE 'Europe/Paris'") :status)
"SET"
ELISP> (pg-result (pg-exec *pg* "SET SESSION TIME ZONE 'America/Chicago'") :status)
"SET"
```

You should see either one or two messages announcing a parameter change (the first statement won't
generate a ParameterStatus message if the time zone was already set to Europe/Paris).

~~~


## Handling asynchronous notifications

PostgreSQL has an asynchronous notification functionality based on the [LISTEN and NOTIFY
commands](https://www.postgresql.org/docs/current/libpq-notify.html). A client can register its
interest in a particular notification channel with the `LISTEN` command, and later stop listening
with the `UNLISTEN` command. All clients listening on a particular channel will be notified
asynchronously when a `NOTIFY` command with that channel name is executed by any client. A “payload”
string can be passed to communicate additional data to the listeners. In pg-el you can register
functions to be called when an asynchronous notification is received by adding them to the
`pg-handle-notice-functions`. Each handler function is called with a single argument, the notice, in
the form of a `pgerror` struct. 


~~~admonish example title="Looking out for DROP TABLE commands"

PostgreSQL will signal an asynchronous notification for a `DROP TABLE IF EXISTS` command that attempts
to remove a table that doesn't exist, as a form of warning message. We can register our interest in
this message by locally binding the `pg-handle-notice-functions` variable.

```lisp
ELISP> (defun deity-p (notif)
         ;; the notification message will be localized, but should contain the table name
         (when (cl-search "deity" (pgerror-message notif))
           (message "Indeed")))
ELISP> (let ((pg-handle-notice-functions (list #'deity-p)))
         (pg-result (pg-exec *pg* "DROP TABLE IF EXISTS deity") :status))
"DROP TABLE"
```

You should see the message in the minibuffer.
~~~


~~~admonish example title="Using NOTIFY / LISTEN"

This example illustrates the use of NOTIFY and LISTEN. It's obviously not very useful with a single
client; real applications would involve multiple event consumers and possibly also multiple event
producers. This functionality can be used to implement simple publish-subscribe communication
patterns, with PostgreSQL serving as an event broker.

```lisp
(cl-flet ((notification-handler (channel payload)
            (message "Async notification on %s: %s" channel payload)))
  (pg-add-notification-handler *pg* #'notification-handler)
  (pg-exec *pg* "LISTEN yourheart")
  (pg-exec *pg* "NOTIFY yourheart, 'foobles'")
  (pg-exec *pg* "SELECT 'ignored'")
  (pg-exec *pg* "NOTIFY yourheart, 'bazzles'")
  (sleep-for 10)
  (pg-exec *pg* "SELECT 'ignored'")
  (pg-exec *pg* "NOTIFY yourheart")
  (pg-exec *pg* "SELECT 'ignored'")
  ;; The function pg_notify is an alternative to the LISTEN statement, and more flexible if your
  ;; channel name is determined by a variable.
  (pg-exec *pg* "SELECT pg_notify('yourheart', 'leaving')")
  (pg-exec *pg* "SELECT 'ignored'")
  (pg-exec *pg* "UNLISTEN yourheart")
  (pg-exec *pg* "NOTIFY yourheart, 'Et redit in nihilum quod fuit ante nihil.'")))
```

~~~

