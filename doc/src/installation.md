# Installation

Install via the [MELPA package archive](https://melpa.org/partials/getting-started.html) by
including the following in your Emacs initialization file (`.emacs.el` or `init.el`):

    (require 'package)
    (add-to-list 'package-archives '("melpa" . "https://melpa.org/packages/") t)

then saying 

     M-x package-install RET pg

To install manually, place the file `pg.el` in a directory on your `load-path`, byte-compile it
(using for example `B` in dired) and add the following to your Emacs initialization file:

    (require 'pg)


