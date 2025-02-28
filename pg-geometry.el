;;; pg-geometry.el --- Support for PostgreSQL geometric types -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-3.0-or-later


;;; Commentary:

;; Geometric data types, per https://www.postgresql.org/docs/current/datatype-geometric.html


;;; Code:

(require 'cl-lib)
(require 'peg)

(declare-function pg-register-parser "pg" (type-name parser))
(declare-function pg-register-textual-serializer "pg" (type-name serializer))
(declare-function pg-initialize-parsers "pg" (con))
(declare-function pg-signal-type-error "pg" (fmt &rest arguments))


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

(defun pg--serialize-point (point _encoding)
  (unless (consp point)
    (pg-signal-type-error "Expecting a cons, got %s" point))
  (unless (numberp (car point))
    (pg-signal-type-error "Expecting a cons of numbers, got %s" point))
  (unless (numberp (cdr point))
    (pg-signal-type-error "Expecting a cons of numbers, got %s" point))
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

(defun pg--serialize-line (line _encoding)
  (unless (vectorp line)
    (pg-signal-type-error "Expecting a vector, got %s" line))
  (unless (numberp (aref line 0))
    (pg-signal-type-error "Expecting a vector of numbers, got %s" line))
  (unless (numberp (aref line 1))
    (pg-signal-type-error "Expecting a vector of numbers, got %s" line))
  (unless (numberp (aref line 2))
    (pg-signal-type-error "Expecting a vector of numbers, got %s" line))
  (format "{%f,%f,%f}" (aref line 0) (aref line 1) (aref line 2)))

;; An lseg is represented in Emacs Lisp by a two-element vector of points.
(defun pg--lseg-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
     ((lseg (* [space]) "[" point "," (* [space]) point (* [space]) "]" (* [space]) (eol)
            `(p1 p2 -- (vector p1 p2)))
      (point "(" x-comma-y ")")
      (x-comma-y (* [space]) float (* [space]) ","
                 (* [space]) float (* [space])
                 `(x y -- (cons x y)))
      (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
             `(str -- (string-to-number str)))
      (sign (or "+" "-" "")))
     (car (peg-run (peg lseg))))))

;; [(x1,y1),(x2,y2)]
(defun pg--serialize-lseg (lseg _encoding)
  (unless (vectorp lseg)
    (pg-signal-type-error "Expecting a vector, got %s" lseg))
  (unless (eql 2 (length lseg))
    (pg-signal-type-error "Expecting a vector of length 2, got %s" lseg))
  (format "[(%f,%f),(%f,%f)]"
          (car (aref lseg 0))
          (cdr (aref lseg 0))
          (car (aref lseg 1))
          (cdr (aref lseg 1))))

;; ( x1 , y1 ) , ( x2 , y2 )
(defun pg--box-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
        ((box (* [space]) point "," (* [space]) point (* [space]) (eol)
               `(p1 p2 -- (vector p1 p2)))
         (point "(" x-comma-y ")")
         (x-comma-y (* [space]) float (* [space]) ","
                    (* [space]) float (* [space])
                    `(x y -- (cons x y)))
         (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
                `(str -- (string-to-number str)))
         (sign (or "+" "-" "")))
      (car (peg-run (peg box))))))

(defun pg--serialize-box (box _encoding)
  (format "(%f,%f),(%f,%f)"
          (car (aref box 0))
          (cdr (aref box 0))
          (car (aref box 1))
          (cdr (aref box 1))))


;; type is one of :open, :closed
(cl-defstruct pg-geometry-path type points)

(defun pg--path-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
     ((path (* [space]) (or open-path closed-path) (* [space]) (eol))
      (open-path "[" (* [space]) (list point-list) (* [space]) "]"
                 `(points -- (make-pg-geometry-path :type :open :points points)))
      (closed-path "(" (* [space]) (list point-list) (* [space]) ")"
                   `(points -- (make-pg-geometry-path :type :closed :points points)))
      (point-list point (* "," (* [space]) point-list))
      (point "(" x-comma-y ")")
      (x-comma-y (* [space]) float (* [space]) ","
                 (* [space]) float (* [space])
                 `(x y -- (cons x y)))
      (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
             `(str -- (string-to-number str)))
      (sign (or "+" "-" "")))
     (car (peg-run (peg path))))))

(defun pg--serialize-path (path encoding)
  (unless (pg-geometry-path-p path)
    (pg-signal-type-error "Expecting a pg-geometry-path object, got %s" path))
  (let ((type (pg-geometry-path-type path))
        (points (pg-geometry-path-points path)))
    (format "%s%s%s"
            (if (eq :open type) "[" "(")
            (string-join (mapcar (lambda (p) (pg--serialize-point p encoding)) points) ",")
            (if (eq :open type) "]" ")"))))

(cl-defstruct pg-geometry-polygon points)

;; ( ( x1 , y1 ) , ... , ( xn , yn ) )
(defun pg--polygon-parser (str _encoding)
  (with-temp-buffer
    (insert str)
    (goto-char (point-min))
    (with-peg-rules
     ((polygon (* [space]) "(" (* [space]) (list point-list) (* [space]) ")" (* [space]) (eol)
               `(points -- (make-pg-geometry-polygon :points points)))
      (point-list point (* "," (* [space]) point-list))
      (point "(" x-comma-y ")")
      (x-comma-y (* [space]) float (* [space]) ","
                 (* [space]) float (* [space])
                 `(x y -- (cons x y)))
      (float (substring sign (+ [digit]) (* "." (+ [digit])) (* "e" sign (+ [digit])))
             `(str -- (string-to-number str)))
      (sign (or "+" "-" "")))
     (car (peg-run (peg polygon))))))

(defun pg--serialize-polygon (polygon encoding)
  (unless (pg-geometry-polygon-p polygon)
    (pg-signal-type-error "Expecting a pg-geometry-polygon object, got %s" polygon))
  (let* ((points (pg-geometry-polygon-points polygon))
         (spoints (mapcar (lambda (p) (pg--serialize-point p encoding)) points)))
    (format "(%s)" (string-join spoints ","))))


(defun pg--geometry-register-serializers ()
  "Register the serializers and deserializers for geometric types."
  (pg-register-parser "point" #'pg--point-parser)
  (pg-register-textual-serializer "point" #'pg--serialize-point)
  (pg-register-parser "line" #'pg--line-parser)
  (pg-register-textual-serializer "line" #'pg--serialize-line)
  (pg-register-parser "lseg" #'pg--lseg-parser)
  (pg-register-textual-serializer "lseg" #'pg--serialize-lseg)
  (pg-register-parser "box" #'pg--box-parser)
  (pg-register-textual-serializer "box" #'pg--serialize-box)
  (pg-register-parser "path" #'pg--path-parser)
  (pg-register-textual-serializer "path" #'pg--serialize-path)
  (pg-register-parser "polygon" #'pg--polygon-parser)
  (pg-register-textual-serializer "polygon" #'pg--serialize-polygon))

(cl-eval-when (load)
  (pg--geometry-register-serializers))

;; We call pg-initialize-parsers to look up the OID corresponding to these newly defined types and
;; to hook them into the parsing machinery.
(defun pg-geometry-setup (con)
  "Initialize (de)serialization support for geometric types.
This function must be called if you loaded the pg-geometry
library after a PostgreSQL connection has been established. It
sets up the deserialization and serialization functionality to
recognize the newly defined types point, line, lseg and so on.
The function need not be called if the pg-geometry library was
loaded prior to establishing your PostgreSQL connection CON."
  (pg--geometry-register-serializers)
  (pg-initialize-parsers con))

(provide 'pg-geometry)

;;; pg-geometry.el ends here
