CREATE TABLE {if_not_exists} {db}.{table}
(
    oid UInt64 CODEC(Delta, LZ4),
    nobs UInt16 CODEC(T64, LZ4),
    ngoodobs UInt16 CODEC(T64, LZ4),
    filter UInt8 CODEC(T64, LZ4),
    fieldid UInt16 CODEC(T64, LZ4),
    rcid UInt8,
    ra Float64,
    dec Float64,
    h3index10 UInt64,
    durgood Float64,
    mingoodmag Float32,
    maxgoodmag Float32,
    meangoodmag Float32
)
ENGINE = MergeTree()
ORDER BY oid
