CREATE TABLE {if_not_exists} {db}.{table}
(
    oid1 UInt64,
    oid2 UInt64
)
ENGINE = MergeTree
ORDER BY oid1
