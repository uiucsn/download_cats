CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns},
    h3index10 UInt64 MATERIALIZED geoToH3(raMean, decMean, 10)
)
ENGINE = MergeTree
ORDER BY objID