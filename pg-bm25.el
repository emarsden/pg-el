;;; pg-bm25.el --- Support for the Vectorchord BM25 extension -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2025  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-3.0-or-later


;;; Commentary:
;;
;; VectorChord-BM25 is a PostgreSQL extension that implements the bm25 (Best Match 25) ranking
;; algorithm, used for information retrieval and search engines. It determines a document’s
;; relevance to a given query and ranks documents based on their relevance scores.
;;
;;    https://github.com/tensorchord/VectorChord-bm25/
;;
;; This file provides parsing and serialization support for the `bm25vector' and `bm25query' types
;; that are implemented by the vchord_bm25 extension.


;;; Code:

(require 'cl-lib)

(declare-function pg-register-parser "pg" (type-name parser))
(declare-function pg-register-textual-serializer "pg" (type-name serializer))
(declare-function pg-exec "pg" (con &rest args))
(declare-function pg-exec-prepared "pg" (con query typed-arguments &rest args))
(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-initialize-parsers "pg" (con))
(declare-function pg-text-parser "pg" (str encoding))
(declare-function pg--serialize-text "pg" (object encoding))


(defun pg--bm25-register-serializers ()
  "Register the (de)serialization functions for Vectorchord BM25 types."
  (pg-register-textual-serializer "bm25vector" #'pg--serialize-text)
  (pg-register-parser "bm25vector" #'pg-text-parser)
  (pg-register-textual-serializer "bm25query" #'pg--serialize-text)
  (pg-register-parser "bm25query" #'pg-text-parser))

(cl-eval-when (load)
  (pg--bm25-register-serializers))

(defun pg-setup-bm25 (con)
  "Prepare for use of Vectorchord BM25 on PostgreSQL connection CON.
Loads the extension, updates the `search_path' to include `bm25_catalog' and
sets up the parsing support for the relevant datatypes.
Return nil if the extension could not be loaded."
  (pg--bm25-register-serializers)
  (condition-case nil
      (progn
        ;; This is a non-privileged extension
        (pg-exec con "CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE")
        (pg-initialize-parsers con)
        (let* ((sql "SELECT current_setting('search_path')")
               (row (pg-result (pg-exec con sql) :tuple 0))
               (new-path (concat (cl-first row) ", bm25_catalog")))
          (pg-exec-prepared con "SELECT set_config('search_path', $1, false)"
                            `((,new-path . "text")))))
    (pg-error nil)))


(provide 'pg-bm25)

;;; pg-bm25.el ends here
