INSERT INTO {circle_db}.{circle_table} SELECT
    arrayJoin(h3kRing(ring.h3index10, toUInt8(ceil(({radius_arcsec} / 3600.) / h3EdgeAngle(10))))) AS h3index10,
    meta.oid AS oid1,
    ring.oid AS oid2,
    ring.filter AS filter2,
    ring.fieldid AS fieldid2,
    greatCircleAngle(meta.ra, meta.dec, ring.ra, ring.dec) AS distance_deg
FROM {meta_db}.{meta_table} AS ring
INNER JOIN
(
    SELECT
        oid,
        ra,
        dec,
        h3index10
    FROM {meta_db}.{meta_table}
    WHERE (ngoodobs > 0) AND (oid >= {begin_oid}) AND (oid < {end_oid})
) AS meta USING (h3index10)
WHERE (distance_deg < ({radius_arcsec} / 3600.)) AND (ring.ngoodobs > 0)
