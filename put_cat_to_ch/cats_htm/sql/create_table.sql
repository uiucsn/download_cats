CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns},
    ra Float64 MATERIALIZED 180.0 / pi() * ra_rad,
    dec Float64 MATERIALIZED 180.0 / pi() * dec_rad,
    sip_hash64_ra_dec UInt64 MATERIALIZED sipHash64(ra, dec),
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10)
)
ENGINE = MergeTree
ORDER BY h3index10
