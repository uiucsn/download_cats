CREATE TABLE {if_not_exists} {db}.{table}
(
    oid UInt64 CODEC(Delta, LZ4),
    fieldid UInt16 CODEC(T64, LZ4),
    ra Float64 CODEC(Gorilla),
    dec Float64 CODEC(Gorilla),
    h3index10 UInt64 CODEC(Delta, LZ4)
)
ENGINE = MergeTree()
PARTITION BY fieldid
ORDER BY oid
