# Data types used by the PostGIS extension

There is deserialization support (and trivial serialization) support for the data types used by the
[PostGIS extension](http://www.postgis.net/). It's necessary to require the `pg-gis` library and to
call `pg-setup-postgis` before using this functionality, to load the extension if necessary and to
set up our deserialization support for the types used by PostGIS (in particular, the `geometry` and
`geography` types).

```lisp
(require 'pg-gis)
(pg-setup-postgis *pg*)
```

PostGIS sends values over the wire in HEXEWKB format ([Extended Well-Known
Binary](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary)
encoded in hexademical), such as `01010000200400000000000000000000000000000000000000` which
represents the well-known text (WKT) `POINT (0 0)`. 

If the variable `pg-gis-use-geosop` is non-nil, we parse this format using the `geosop` commandline
utility function from GEOS (often available in packages named `geos-bin` or similar). Otherwise, we
leave it as a string (it can be parsed using PostGIS functions such as `ST_AsText`).

```shell
sudo apt install geos-bin
```


~~~admonish example title="Using PostGIS datatypes"
```lisp
ELISP> (require 'pg-gis)
ELISP> (pg-setup-postgis *pg*)
ELISP> (pg-result (pg-exec *pg* "SELECT 'POINT(4 5)'::geometry") :tuple 0)
("POINT (4 5)")
ELISP> (pg-result (pg-exec *pg* "SELECT Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))") :tuple 0)
("BOX(1 2,5 6)")
ELISP> (pg-result (pg-exec *pg* "SELECT 'MULTILINESTRING((-118.584 38.374 20,-118.583 38.5 30),(-71.05957 42.3589 75, -71.061 43 90))'::geometry") :tuple 0)
("MULTILINESTRING Z ((-118.584 38.374 20, -118.583 38.5 30), (-71.05957 42.3589 75, -71.061 43 90))")
ELISP> (pg-result (pg-exec *pg* "SELECT 'SPHEROID[\"GRS_1980\",6378137,298.2572]'::spheroid") :tuple 0)
("SPHEROID(\"GRS_1980\",6378137,298.2572)")
```

~~~

