INSERT INTO {xmatch_db}.{xmatch_table} SELECT
    oid1,
    argMin(oid2, distance_deg) AS oid2,
    argMin(ra, distance_deg) AS ra2,
    argMin(dec, distance_deg) AS dec2
FROM {circle_db}.{circle_table}
GROUP BY
    oid1,
    filter2,
    fieldid2
