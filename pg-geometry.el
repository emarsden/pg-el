;;; pg-geometry.el --- Support for PostgreSQL geometric types -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;;
;; Geometric data types, per https://www.postgresql.org/docs/current/datatype-geometric.html

(require 'cl-lib)


(defun pg--point-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
     ((point (or with-parens without-parens))
      (with-parens (* [space]) (* "(") x-comma-y (* ")") (* [space]) (eol))
      (without-parens x-comma-y (* [space]) (eol))
      (x-comma-y (* [space]) float (* [space]) ","
                 (* [space]) float (* [space]) (* ")")
                 `(x y -- (cons x y)))
      (float (substring (+ [digit]) (* "." (+ [digit])) (* "e" (+ [digit])))
             `(str -- (string-to-number str))))
     (car (peg-run (peg point))))))


(defun pg--serialize-point (point)
  (cl-assert (consp point) t)
  (cl-assert (numberp (car point)) t)
  (cl-assert (numberp (cdr point)) t)
  (format "(%s,%s)" (car point) (cdr point)))


(eval-after-load 'pg
  (progn
    (pg-register-parser "point" #'pg--point-parser)
    (pg-register-textual-serializer "point" #'pg--serialize-point)))


(provide 'pg-geometry)
