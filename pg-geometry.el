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
      (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
             `(str -- (string-to-number str)))
      (sign (or "+" "-" "")))
     (car (peg-run (peg point))))))

(defun pg--serialize-point (point)
  (cl-assert (consp point) t)
  (cl-assert (numberp (car point)) t)
  (cl-assert (numberp (cdr point)) t)
  (format "(%s,%s)" (car point) (cdr point)))

;; A line is represented in Emacs Lisp by a 3-element vector.
(defun pg--line-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
     ((line (* [space]) "{" float ","
            (* [space]) float ","
            (* [space]) float "}" (* [space]) (eol)
            `(a b c -- (vector a b c)))
      (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
             `(str -- (string-to-number str)))
      (sign (or "+" "-" "")))
     (car (peg-run (peg line))))))

(defun pg--serialize-line (line)
  (cl-assert (vectorp line))
  (cl-assert (numberp (aref line 0)))
  (cl-assert (numberp (aref line 1)))
  (cl-assert (numberp (aref line 2)))
  (format "{%f,%f,%f}" (aref line 0) (aref line 1) (aref line 2)))

(eval-after-load 'pg
  (progn
    (pg-register-parser "point" #'pg--point-parser)
    (pg-register-textual-serializer "point" #'pg--serialize-point)
    (pg-register-parser "line" #'pg--line-parser)
    (pg-register-textual-serializer "line" #'pg--serialize-line)))


(provide 'pg-geometry)
