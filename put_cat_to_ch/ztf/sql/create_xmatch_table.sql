CREATE TABLE {if_not_exists} {xmatch_db}.{xmatch_table}
ENGINE = MergeTree
ORDER BY (oid1, oid2) AS
SELECT
    oid1,
    argMin(oid2, distance) AS oid2
FROM
(
    SELECT
        ring.oid AS oid1,
        meta.oid AS oid2,
        meta.filter AS filter2,
        meta.fieldid AS fieldid2,
        greatCircleAngle(meta.ra, meta.dec, ring.ra, ring.dec) AS distance
    FROM {meta_db}.{meta_table} AS meta
    INNER JOIN
    (
        SELECT
            oid,
            ra,
            dec,
            arrayJoin(h3kRing(h3index10, toUInt8(ceil(({radius_arcsec} / 3600.) / h3EdgeAngle(10))))) AS h3index10
        FROM {meta_db}.{meta_table}
        WHERE ngoodobs > 0
    ) AS ring USING (h3index10)
    WHERE (distance < ({radius_arcsec} / 3600.)) AND (oid1 <= oid2) AND (meta.ngoodobs > 0)
)
GROUP BY
    oid1,
    filter2,
    fieldid2
