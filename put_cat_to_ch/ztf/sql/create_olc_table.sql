CREATE TABLE {if_not_exists} {db}.{table}
(
    oid UInt64 CODEC(Delta, LZ4),
    filter UInt8 CODEC(T64, LZ4),
    fieldid UInt16 CODEC(T64, LZ4),
    rcid UInt8 CODEC(Delta, LZ4),
    ra Float64 CODEC(Gorilla),
    dec Float64 CODEC(Gorilla),
    nobs_w_bad UInt16 CODEC(T64, LZ4),
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10) CODEC(Delta, LZ4),
    mjd Array(Float64),
    mag Array(Float32),
    magerr Array(Float32),
    clrcoeff Array(Float32),
    ngoodobs UInt16 MATERIALIZED length(mjd) CODEC(T64, LZ4),
    durgood Float64 MATERIALIZED arrayMax(mjd) - arrayMin(mjd) CODEC(Gorilla),
    mingoodmag Float32 MATERIALIZED arrayMin(mag) CODEC(Gorilla),
    maxgoodmag Float32 MATERIALIZED arrayMax(mag) CODEC(Gorilla),
    meangoodmag Float32 MATERIALIZED arrayAvg(mag) CODEC(Gorilla)
)
ENGINE = MergeTree()
PARTITION BY fieldid
ORDER BY (h3index10, oid)
PRIMARY KEY h3index10
