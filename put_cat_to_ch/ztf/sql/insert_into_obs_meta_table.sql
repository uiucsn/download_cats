INSERT INTO {meta_db}.{meta_table}
SELECT
    oid,
    any(nobs) as nobs,
    countIf(catflags = 0) AS ngoodobs,
    any(filter) as filter,
    any(fieldid) as fieldid,
    any(rcid) as rcid,
    any(ra) as ra,
    any(dec) as dec,
    any(h3index10) as h3index10,
    (maxIf(mjd, catflags = 0) - minIf(mjd, catflags = 0)) AS durgood,
    (minIf(mag, catflags = 0)) as mingoodmag,
    (maxIf(mag, catflags = 0)) as maxgoodmag,
    (avgIf(mag, catflags = 0)) as avggoodmag
FROM {obs_db}.{obs_table}
GROUP BY oid
