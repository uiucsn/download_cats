CREATE TABLE {if_not_exists} {db}.{table}
(
    {columns},
    objid UInt64 MATERIALIZED (
        bitShiftLeft(toUInt64(2), 59)
        + bitShiftLeft(toUInt64(rerun), 48)
        + bitShiftLeft(toUInt64(run), 32)
        + bitShiftLeft(toUInt64(camcol), 29)
        + bitShiftLeft(toUInt64(field), 16)
        + toUInt64(id)
    ),
    long_objc_flags UInt64 MATERIALIZED bitShiftLeft(toUInt64(objc_flags2), 32) + toUInt32(objc_flags),
    long_flags_u UInt64 MATERIALIZED bitShiftLeft(toUInt64(flags2_u), 32) + toUInt32(flags_u),
    long_flags_g UInt64 MATERIALIZED bitShiftLeft(toUInt64(flags2_g), 32) + toUInt32(flags_g),
    long_flags_r UInt64 MATERIALIZED bitShiftLeft(toUInt64(flags2_r), 32) + toUInt32(flags_r),
    long_flags_i UInt64 MATERIALIZED bitShiftLeft(toUInt64(flags2_i), 32) + toUInt32(flags_i),
    long_flags_z UInt64 MATERIALIZED bitShiftLeft(toUInt64(flags2_z), 32) + toUInt32(flags_z),
    h3index7 UInt64 MATERIALIZED geoToH3(ra, dec, 7),
    h3index10 UInt64 MATERIALIZED geoToH3(ra, dec, 10)
)
ENGINE = MergeTree
ORDER BY h3index10
