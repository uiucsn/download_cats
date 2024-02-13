CREATE TABLE {if_not_exists} {db}.{table}
ENGINE = MergeTree
PRIMARY KEY expid AS
SELECT
    expid,
    any(field) AS field,
    any(rcid) AS rcid,
    any(filter) AS filter,
    any(exptime) AS exptime,
    any(airmass) AS airmass,
    any(infobits) AS infobits,
    any(dr) AS dr,
    any(numsci) AS numsci,
    any(numdiff) AS numdiff,
    any(fwhm) AS fwhm,
    any(maglim) AS maglim,
    any(scibckgnd) AS scibckgnd,
    any(ellip) AS ellip,
    any(ellippa) AS ellippa,
    any(expstart_mjd) AS expstart_mjd
FROM {db_exposures}.{table_exposures}
GROUP BY expid