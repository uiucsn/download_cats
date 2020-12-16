CREATE TABLE {if_not_exists} {db}.{table}
(
    h3index10 UInt64,
    oid1 UInt64,
    oid2 UInt64,
    filter2 UInt8,
    fieldid2 UInt16,
    ra Float64,
    dec Float64,
    distance_deg Float64
)
ENGINE = MergeTree()
ORDER BY oid1
