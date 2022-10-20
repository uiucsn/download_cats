INSERT INTO {olc_db}.{olc_table}
SELECT
    oid,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
    nobs_w_bad,
    arrayFilter((_, is_good) -> is_good, mjd, mask) AS mjd,
    arrayFilter((_, is_good) -> is_good, mag, mask) AS mag,
    arrayFilter((_, is_good) -> is_good, magerr, mask) AS magerr,
    arrayFilter((_, is_good) -> is_good, clrcoeff, mask) AS clrcoeff
FROM
(
    SELECT
        toUInt64(objectid) AS oid,
        filterid AS filter,
        fieldid,
        rcid,
        toFloat64(objra) AS ra,
        toFloat64(objdec) AS dec,
        length(hmjd) AS nobs_w_bad,
        hmjd AS mjd,
        mag,
        magerr,
        clrcoeff,
        catflags,
        arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags) AS mask
    FROM {parquet_db}.{parquet_table}
)
WHERE length(mag) > 0
