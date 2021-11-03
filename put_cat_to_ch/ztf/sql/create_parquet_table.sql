CREATE TABLE {if_not_exists} {db}.{table}
(
    `objectid` Int64,
    `filterid` UInt8,
    `fieldid` UInt16,
    `rcid` UInt8,
    `objra` Float32,
    `objdec` Float32,
    `nepochs` Int64,
    `hmjd` Array(Float64),
    `mag` Array(Float32),
    `magerr` Array(Float32),
    `clrcoeff` Array(Float32),
    `catflags` Array(UInt16)
)
ENGINE = MergeTree
PARTITION BY fieldid
ORDER BY objectid
