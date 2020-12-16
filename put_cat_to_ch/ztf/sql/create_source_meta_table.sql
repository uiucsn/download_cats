CREATE TABLE {if_not_exists} {db}.{table}
(
    sid UInt64 CODEC(Delta, LZ4),
    ra Float64,
    dec Float64,
    h3index10 UInt64,
    oids Array(UInt64),
    filters Array(UInt8),
    fieldids Array(UInt16),
    nobs UInt16 CODEC(T64, LZ4),
    nobs_g UInt16 CODEC(T64, LZ4),
    nobs_r UInt16 CODEC(T64, LZ4),
    nobs_i UInt16 CODEC(T64, LZ4),
    max_mag_g Nullable(Float32),
    max_mag_r Nullable(Float32),
    max_mag_i Nullable(Float32),
    min_mag_g Nullable(Float32),
    min_mag_r Nullable(Float32),
    min_mag_i Nullable(Float32),
    mean_mag_g Nullable(Float32),
    mean_mag_r Nullable(Float32),
    mean_mag_i Nullable(Float32),
    duration_g Float32,
    duration_r Float32,
    duration_i Float32,
    duration_gr_wide Float32,
    duration_gr_narrow Float32,
    duration_gri_wide Float32,
    duration_gri_narrow Float32
)
ENGINE = MergeTree
ORDER BY sid
