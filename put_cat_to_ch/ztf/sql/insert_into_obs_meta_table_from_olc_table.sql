INSERT INTO {meta_db}.{meta_table}
SELECT
    oid,
    nobs_w_bad AS nobs,
    ngoodobs,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
    h3index10,
    durgood,
    mingoodmag,
    maxgoodmag,
    meangoodmag
FROM {olc_db}.{olc_table}
