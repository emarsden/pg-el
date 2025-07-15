# Installation

You can install via the NonGNU ELPA package archive, or via the MELPA package archive, or with
`package-vc-install`, or with `use-package`.


## Installing via NonGNU ELPA

From Emacs, run

    M-x package-install RET pg

to install the pg package from the [NonGNU ELPA](https://elpa.nongnu.org/) package archive.


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

With Emacs 29, you can install the library from the latest Github revision (this requires git to be
installed) using:

     (unless (package-installed-p 'pg)
        (package-vc-install "https://github.com/emarsden/pg-el" nil nil 'pg))

You can later update to the latest version with `M-x package-vc-upgrade RET pg RET`.


## Installing with `use-package`

If you prefer to use the `use-package` macro, which is built in to Emacs 29, you can use (requires
git to be installed):

    (use-package pg :vc (:url "https://github.com/emarsden/pg-el"))

