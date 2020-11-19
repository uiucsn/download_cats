CREATE TABLE {if_not_exists} {xmatch_db}.{xmatch_table}
ENGINE = MergeTree
ORDER BY oid1 AS
SELECT
    oid1,
    argMin(oid2, distance) AS oid2
FROM
(
    SELECT
        arrayJoin(h3kRing(ring.h3index10, toUInt8(ceil(({radius_arcsec} / 3600.) / h3EdgeAngle(10))))) AS h3index10,
        meta.oid AS oid1,
        ring.oid AS oid2,
        ring.filter AS filter2,
        ring.fieldid AS fieldid2,
        greatCircleAngle(meta.ra, meta.dec, ring.ra, ring.dec) AS distance
    FROM {meta_db}.{meta_table} AS ring
    INNER JOIN
    (
        SELECT
            oid,
            ra,
            dec,
            h3index10
        FROM {meta_db}.{meta_table}
        WHERE ngoodobs > 0
    ) AS meta USING (h3index10)
    WHERE (distance < ({radius_arcsec} / 3600.)) AND (ring.ngoodobs > 0)
)
GROUP BY
    oid1,
    filter2,
    fieldid2
