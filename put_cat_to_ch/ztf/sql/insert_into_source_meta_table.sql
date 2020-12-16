INSERT INTO {source_meta_db}.{source_meta_table}
SELECT
    sid,
    any(ra),
    any(dec),
    any(h3index10),
    arraySort(groupUniqArray(oid)) AS oids,
    arraySort(groupUniqArray(filter)) AS filters,
    arraySort(groupUniqArray(fieldid)) AS fieldids,
    count() AS nobs,
    countIf(filter = 1) AS nobs_g,
    countIf(filter = 2) AS nobs_r,
    countIf(filter = 3) AS nobs_i,
    if(nobs_g > 0, maxIf(mag, filter = 1), NULL) AS max_mag_g,
    if(nobs_r > 0, maxIf(mag, filter = 2), NULL) AS max_mag_r,
    if(nobs_i > 0, maxIf(mag, filter = 3), NULL) AS max_mag_i,
    if(nobs_g > 0, minIf(mag, filter = 1), NULL) AS min_mag_g,
    if(nobs_r > 0, minIf(mag, filter = 2), NULL) AS min_mag_r,
    if(nobs_i > 0, minIf(mag, filter = 3), NULL) AS min_mag_i,
    if(nobs_g > 0, avgIf(mag, filter = 1), NULL) AS mean_mag_g,
    if(nobs_r > 0, avgIf(mag, filter = 2), NULL) AS mean_mag_r,
    if(nobs_i > 0, avgIf(mag, filter = 3), NULL) AS mean_mag_i,
    if(nobs_g > 0, (maxIf(mjd, filter = 1) - minIf(mjd, filter = 1)), 0) AS duration_g,
    if(nobs_r > 0, (maxIf(mjd, filter = 2) - minIf(mjd, filter = 2)), 0) AS duration_r,
    if(nobs_i > 0, (maxIf(mjd, filter = 3) - minIf(mjd, filter = 3)), 0) AS duration_i,
    if((nobs_g > 0) OR (nobs_r > 0), (maxIf(mjd, (filter = 1) OR (filter = 2)) - minIf(mjd, (filter = 1) OR (filter = 2))), 0) AS duration_gr_wide,
    greatest(
        if(
            (nobs_g > 0) AND (nobs_r) > 0,
            least(maxIf(mjd, filter = 1), maxIf(mjd, filter = 2)) - greatest(minIf(mjd, filter = 1), minIf(mjd, filter = 2)),
            0
        ),
        0
    ) AS duration_gr_narrow,
    (max(mjd) - min(mjd)) AS duration_gri_wide,
    greatest(
        (arrayReduce('min', [maxIf(mjd, filter = 1), maxIf(mjd, filter = 2), maxIf(mjd, filter = 3)]) - arrayReduce('max', [minIf(mjd, filter = 1), minIf(mjd, filter = 2), minIf(mjd, filter = 3)])),
        0
    ) AS duration_gri_narrow
FROM {source_obs_db}.{source_obs_table}
GROUP BY sid
