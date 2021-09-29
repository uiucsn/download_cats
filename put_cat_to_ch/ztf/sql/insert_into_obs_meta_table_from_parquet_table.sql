INSERT INTO {meta_db}.{meta_table}
SELECT
    toUInt64(objectid) AS oid,
    nepochs AS nobs,
    arraySum(arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) AS ngoodobs,
    filterid AS filter,
    fieldid,
    rcid,
    toFloat64(objra) AS ra,
    toFloat64(objdec) AS dec,
    geoToH3(ra, dec, 10) AS h3index10,
    arrayReduce('maxIf', hmjd, arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) - arrayReduce('minIf', hmjd, arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) AS durgood,
    arrayReduce('minIf', mag, arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) AS mingoodmag,
    arrayReduce('maxIf', mag, arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) AS maxgoodmag,
    arrayReduce('avgIf', mag, arrayMap((err, flag) -> ((err > 0) AND (flag = 0)), magerr, catflags)) AS avggoodmag
FROM {obs_db}.{obs_table}
WHERE arrayExists(err -> (err > 0), magerr) AND arrayExists(flag -> (flag = 0), catflags)
