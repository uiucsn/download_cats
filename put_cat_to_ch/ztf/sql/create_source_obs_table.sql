CREATE TABLE {if_not_exists} {db}.{table}
(
    sid UInt64 Codec(Delta, LZ4),
    oid UInt64 CODEC(Delta, LZ4),
    filter UInt8 CODEC(T64, LZ4),
    fieldid UInt16 CODEC(T64, LZ4),
    rcid UInt8 CODEC(Delta, LZ4),
    ra Float64 CODEC(Gorilla),
    dec Float64 CODEC(Gorilla),
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10) CODEC(Delta, LZ4),
    mjd Float64,
    mag Float32,
    magerr Float32,
    clrcoeff Float32
)
ENGINE = MergeTree()
ORDER BY (sid, mjd)
PRIMARY KEY sid
