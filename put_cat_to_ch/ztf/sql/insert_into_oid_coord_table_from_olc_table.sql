INSERT INTO {oid_coord_db}.{oid_coord_table}
SELECT
    oid,
    fieldid,
    ra,
    dec,
    h3index10
FROM {olc_db}.{olc_table}
