INSERT INTO {obs_db}.{obs_table}
SELECT
    oid,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
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
        hmjd AS mjd,
        mag,
        magerr,
        clrcoeff,
        catflags,
        arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags) AS mask
    FROM {parquet_db}.{parquet_table}
)
WHERE length(mag) > 0
