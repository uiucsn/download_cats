CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns},
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10)
)
ENGINE = MergeTree
ORDER BY h3index10