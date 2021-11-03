INSERT INTO {obs_db}.{obs_table}
SELECT
    toUInt64(objectid) AS oid,
    toUInt16(nepochs) AS nobs,
    filterid AS filter,
    fieldid,
    rcid,
    toFloat64(objra) AS ra,
    toFloat64(objdec) AS dec,
    hmjd AS mjd,
    mag,
    magerr,
    clrcoeff,
    catflags
FROM {parquet_db}.{parquet_table}
ARRAY JOIN
    hmjd,
    mag,
    magerr,
    clrcoeff,
    catflags
