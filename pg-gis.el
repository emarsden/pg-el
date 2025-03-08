;;; pg-gis.el --- Support for PostGIS types -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-3.0-or-later


;;; Commentary:

;; PostGIS
;; https://postgis.net/docs/manual-3.4/using_postgis_dbmanagement.html

;;; Code:

(require 'cl-lib)

(declare-function pg-register-parser "pg" (type-name parser))
(declare-function pg-register-textual-serializer "pg" (type-name serializer))
(declare-function pg-exec "pg" (con &rest args))
(declare-function pg-initialize-parsers "pg" (con))
(declare-function pg--serialize-text "pg" (object encoding))


(defvar pg-gis-use-geosop t
  "If non-nil, parse PostGIS EWKB to text using the geosop utility.")


;; PostGIS sends values over the wire in HEXEWKB format (Extended Well-Known Binary encoded in
;; hexademical), such as "01010000200400000000000000000000000000000000000000".
;;
;;   https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
;;
;; if the variable `pg-gis-use-geosop' is non-nil, we parse this format using the geosop commandline
;; utility function from GEOS (often available in packages named geos-bin or similar). Otherwise, we
;; leave it as a string (it can be parsed using PostGIS functions such as ST_AsText).
;;
;; Some alternative parsing code that we could adapt to elisp:
;;   https://github.com/filonenko-mikhail/cl-ewkb/blob/master/cl-ewkb/ewkb.lisp
;;
;; % echo 01010000200400000000000000000000000000000000000000 | geosop -a stdin.wkb -f txt
;; POINT (0 0)
(defun pg-gis--parse-ewkb-geosop (string _encoding)
  "Parse STRING in EWKB or HEXEWKB to text using the geosop application."
  (with-temp-buffer
    (call-process-region string nil "geosop" nil t nil "-a" "stdin.wkb" "-f" "txt")
    (string-trim (buffer-string))))

(defun pg-gis--parse-ewkb (string encoding)
  "Parse STRING in EWKB or HEXEWKB following the value of pg-gis-use-geosop."
  (if pg-gis-use-geosop
      (pg-gis--parse-ewkb-geosop string encoding)
    string))

(defun pg-gis--parse-spheroid (string _encoding)
  string)

(defun pg-gis--parse-box2d (string _encoding)
  string)


(defun pg-gis--parse-box3d (string _encoding)
  string)

;; PostGIS data types that we receive over the wire:
;;
;;  geometry
;;  geography
;;  box2d
;;  box3d
;;  spheroid

;; Types that don't seem to be sent over the wire:
;;  box2df  -- box2d with floating point precision
;;  gidx    -- box3d with floating point precision
;;  geometry_dump
;;  valid_detail
;;  geography_columns

(defun pg--gis-register-serializers ()
  "Register the (de)serialization functions for PostGIS types."
  (pg-register-parser "geometry" #'pg-gis--parse-ewkb)
  (pg-register-textual-serializer "geometry" #'pg--serialize-text)
  (pg-register-parser "geography" #'pg-gis--parse-ewkb)
  (pg-register-textual-serializer "geography" #'pg--serialize-text)
  (pg-register-parser "spheroid" #'pg-gis--parse-spheroid)
  (pg-register-textual-serializer "spheroid" #'pg--serialize-text)
  (pg-register-parser "box2d" #'pg-gis--parse-box2d)
  (pg-register-textual-serializer "box2d" #'pg--serialize-text)
  (pg-register-parser "box3d" #'pg-gis--parse-box3d)
  (pg-register-textual-serializer "box3d" #'pg--serialize-text))

(cl-eval-when (load)
  (pg--gis-register-serializers))

;; This function must be called before using the PostGIS extension. It loads the extension if
;; necessary, and sets up the parsing support for the relevant datatypes.
(defun pg-setup-postgis (con)
  "Prepare for use of PostGIS types on PostgreSQL connection CON.
Return nil if the extension could not be loaded."
  (pg--gis-register-serializers)
  (condition-case nil
      (progn
        (pg-exec con "CREATE EXTENSION IF NOT EXISTS postgis")
        (pg-initialize-parsers con))
    (pg-error nil)))


(provide 'pg-gis)

;;; pg-gis.el ends here
