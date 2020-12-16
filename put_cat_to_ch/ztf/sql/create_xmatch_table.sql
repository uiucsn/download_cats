CREATE TABLE {if_not_exists} {db}.{table}
(
    oid1 UInt64,
    oid2 UInt64,
    ra2 Float64,
    dec2 Float64
)
ENGINE = MergeTree
ORDER BY oid1
