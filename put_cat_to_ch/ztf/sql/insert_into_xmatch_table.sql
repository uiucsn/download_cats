INSERT INTO {xmatch_db}.{xmatch_table} SELECT
    oid1,
    argMin(oid2, distance_deg) AS oid2
FROM {circle_db}.{circle_table}
GROUP BY
    oid1,
    filter2,
    fieldid2
