CREATE TABLE {if_not_exists} {lc_db}.{lc_table}
(
    `ra` Float64,
    `dec` Float64,
    `h3index10` UInt64 MATERIALIZED geoToH3(ra, dec, 10),
    `lc` Array(Tuple(Float64, Float32, Float32, UInt64, UInt8)),
    `lc_g` Array(Tuple(Float64, Float32, Float32)) MATERIALIZED arraySort(x -> (x.1), arrayMap(x -> (x.1, x.2, x.3), arrayFilter(x -> ((x.5) = 1), lc))),
    `lc_r` Array(Tuple(Float64, Float32, Float32)) MATERIALIZED arraySort(x -> (x.1), arrayMap(x -> (x.1, x.2, x.3), arrayFilter(x -> ((x.5) = 2), lc))),
    `lc_i` Array(Tuple(Float64, Float32, Float32)) MATERIALIZED arraySort(x -> (x.1), arrayMap(x -> (x.1, x.2, x.3), arrayFilter(x -> ((x.5) = 3), lc))),
    `oid` Array(UInt64) MATERIALIZED arraySort(arrayReduce('groupUniqArray', lc.4)),
    `filter` Array(UInt8) MATERIALIZED arraySort(arrayReduce('groupUniqArray', lc.5)),
    `nobs` UInt16 MATERIALIZED length(lc),
    `nobs_g` UInt16 MATERIALIZED length(lc_g),
    `nobs_r` UInt16 MATERIALIZED length(lc_r),
    `nobs_i` UInt16 MATERIALIZED length(lc_i),
    `max_mag_g` Float32 MATERIALIZED arrayReduce('max', lc_g.2),
    `max_mag_r` Float32 MATERIALIZED arrayReduce('max', lc_r.2),
    `max_mag_i` Float32 MATERIALIZED arrayReduce('max', lc_i.2),
    `min_mag_g` Float32 MATERIALIZED arrayReduce('min', lc_g.2),
    `min_mag_r` Float32 MATERIALIZED arrayReduce('min', lc_r.2),
    `min_mag_i` Float32 MATERIALIZED arrayReduce('min', lc_i.2),
    `mean_mag_g` Float32 MATERIALIZED arrayReduce('avg', lc_g.2),
    `mean_mag_r` Float32 MATERIALIZED arrayReduce('avg', lc_r.2),
    `mean_mag_i` Float32 MATERIALIZED arrayReduce('avg', lc_i.2),
    `duration_g` Float32 MATERIALIZED if(nobs_g > 0, ((lc_g.1)[-1]) - ((lc_g.1)[1]), 0),
    `duration_r` Float32 MATERIALIZED if(nobs_r > 0, ((lc_r.1)[-1]) - ((lc_r.1)[1]), 0),
    `duration_i` Float32 MATERIALIZED if(nobs_i > 0, ((lc_i.1)[-1]) - ((lc_i.1)[1]), 0),
    `duration_wide` Float32 MATERIALIZED arrayReduce('max', lc.1) - arrayReduce('min', lc.1),
    `duration_narrow` Float32 MATERIALIZED greatest(if((nobs_g > 0) AND (nobs_r > 0) AND (nobs_i > 0), arrayReduce('min', [(lc_g.1)[-1], (lc_r.1)[-1], (lc_i.1)[-1]]) - arrayReduce('max', [(lc_g.1)[1], (lc_r.1)[1], (lc_i.1)[1]]), 0), 0)
)
ENGINE = MergeTree
ORDER BY h3index10
