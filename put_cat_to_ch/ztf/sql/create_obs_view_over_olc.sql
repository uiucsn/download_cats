CREATE VIEW IF NOT EXISTS {view_db}.{view_table} AS SELECT
    oid,
    nobs_w_bad AS nobs,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
    h3index10,
    mjd,
    mag,
    magerr,
    clrcoeff,
    0 AS catflags
FROM {olc_db}.{olc_table}
ARRAY JOIN mjd, mag, magerr, clrcoeff
ORDER BY h3index10, oid, mjd