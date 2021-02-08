CREATE TABLE {db}.{table}
ENGINE = Set() AS
SELECT oid2
FROM {xmatch_db}.{xmatch_table}
WHERE oid1 < oid2
