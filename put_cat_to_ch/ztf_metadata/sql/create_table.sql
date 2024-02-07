CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns}
)
ENGINE = MergeTree
ORDER BY expid