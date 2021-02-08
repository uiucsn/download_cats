INSERT INTO {source_obs_db}.{source_obs_table} SELECT
    match.oid1 AS sid,
    obs.oid AS oid,
    obs.filter AS filter,
    obs.fieldid AS fieldid,
    obs.rcid AS rcid,
    coord.ra AS ra,
    coord.dec AS dec,
    obs.mjd AS mjd,
    obs.mag AS mag,
    obs.magerr AS magerr,
    obs.clrcoeff AS clrcoeff
FROM {obs_db}.{obs_table} AS obs
INNER JOIN
(
    SELECT
        oid1,
        oid2
    FROM {xmatch_db}.{xmatch_table}
    WHERE (oid1 <= oid2) AND (oid1 NOT IN {oids2_db}.{oids2_table}) AND (oid1 >= {begin_oid}) AND (oid1 < {end_oid})
) AS match ON match.oid2 = obs.oid
INNER JOIN (
    WITH
        (pi() / 180.0) AS deg_to_rad,
        (180.0 / pi()) AS rad_to_deg
    SELECT
        oid1,
        rad_to_deg * atan2(sum(sin(deg_to_rad * ra2)), sum(cos(deg_to_rad * ra2))) AS ra,
        rad_to_deg * atan2(sum(sin(deg_to_rad * dec2)), sum(cos(deg_to_rad * dec2))) AS dec
    FROM {xmatch_db}.{xmatch_table}
    WHERE (oid1 <= oid2) AND (oid1 NOT IN {oids2_db}.{oids2_table}) AND (oid1 >= {begin_oid}) AND (oid1 < {end_oid})
    GROUP BY oid1
) AS coord ON match.oid1 = coord.oid1
WHERE (obs.catflags = 0) AND (obs.magerr > 0)
