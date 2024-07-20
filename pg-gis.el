;;; pg-gis.el --- Support for PostGIS types -*- lexical-binding: t -*-
;;
;; Copyright: (C) 2024  Eric Marsden
;; Author: Eric Marsden <eric.marsden@risk-engineering.org>
;; SPDX-License-Identifier: GPL-2.0-or-later


;;; Commentary:

;; PostGIS
;; https://postgis.net/docs/manual-3.4/using_postgis_dbmanagement.html

;;; Code:

(require 'cl-lib)

(declare-function pg-register-parser "pg" (type-name parser))
(declare-function pg-register-textual-serializer "pg" (type-name serializer))


(defvar pg-gis-use-geosop t
  "If non-nil, parse PostGIS EWKB format to text using the geosop commandline application.")


;; This function must be called before using the PostGIS extension. It loads the extension if
;; necessary, and sets up the parsing support for the relevant datatypes.
(defun pg-setup-postgis (con)
  "Prepare for use of PostGIS types on PostgreSQL connection CON.
Return nil if the extension could not be loaded."
  (condition-case nil
      (pg-exec con "CREATE EXTENSION IF NOT EXISTS postgis")
    (pg-error nil)))


;; PostGIS sends values over the wire in HEXEWKB format (Extended Well-Known Binary encoded in
;; hexidemical), such as "01010000200400000000000000000000000000000000000000".
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
(defun pg-gis--parse-ewkb-geosop (str _encoding)
  "Parse a string in EWKB or HEXEWKB to text using the geosop application."
  (with-temp-buffer
    (call-process-region str nil "geosop" nil t nil "-a" "stdin.wkb" "-f" "txt")
    (string-trim (buffer-string))))

(defun pg-gis--parse-ewkb (str encoding)
  "Parse a string in EWKB or HEXEWKB following the value of pg-gis-use-geosop."
  (if pg-gis-use-geosop
      (pg-gis--parse-ewkb-geosop str encoding)
    str))

(defun pg-gis--parse-spheroid (str _encoding)
  str)

(defun pg-gis--parse-box2d (str _encoding)
  str)


(defun pg-gis--parse-box3d (str _encoding)
  str)

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


(pg-register-parser "geometry" #'pg-gis--parse-ewkb)
(pg-register-textual-serializer "geometry" #'identity)
(pg-register-parser "geography" #'pg-gis--parse-ewkb)
(pg-register-textual-serializer "geography" #'identity)
(pg-register-parser "spheroid" #'pg-gis--parse-spheroid)
(pg-register-textual-serializer "spheroid" #'identity)
(pg-register-parser "box2d" #'pg-gis--parse-box2d)
(pg-register-textual-serializer "box2d" #'identity)
(pg-register-parser "box3d" #'pg-gis--parse-box3d)
(pg-register-textual-serializer "box3d" #'identity)


(provide 'pg-gis)

;;; pg-gis.el ends here
