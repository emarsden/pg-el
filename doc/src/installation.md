# Installation

You can install via the MELPA package archive, or with package-vc-install, or manually.


## Installing via MELPA

Install via the [MELPA package archive](https://melpa.org/partials/getting-started.html) by
including the following in your Emacs initialization file (`.emacs.el` or `init.el`):

```lisp
(require 'package)
(add-to-list 'package-archives '("melpa" . "https://melpa.org/packages/") t)
```

then saying 

     M-x package-install RET pg


## Installing with package-vc-install

You can install the library from the latest Github revision using:

     (unless (package-installed-p 'pg)
        (package-vc-install "https://github.com/emarsden/pg-el" nil nil 'pg))

You can later update to the latest version with `M-x package-vc-upgrade RET pg RET`.


## Installing manually

To install manually, place the file `pg.el` in a directory on your `load-path`, byte-compile it
(using for example `B` in dired) and add the following to your Emacs initialization file:

```lisp
(require 'pg)
```

