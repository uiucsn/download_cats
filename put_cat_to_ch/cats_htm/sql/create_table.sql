CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns},
    id UInt64 MATERIALIZED sipHash64(ra, dec),
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10)
)
ENGINE = MergeTree
ORDER BY h3index10
