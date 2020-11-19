INSERT INTO {lc_db}.{lc_table} SELECT
    any(obs.ra) AS ra,
    any(obs.dec) AS dec,
    arraySort(x -> (x.1), groupArray((obs.mjd, obs.mag, obs.magerr, obs.oid, obs.filter))) AS lc
FROM {obs_db}.{obs_table} AS obs
INNER JOIN
(
    SELECT
        oid1,
        oid2
    FROM {xmatch_db}.{xmatch_table}
    WHERE (oid1 = oid2) OR (
        (oid1 < oid2) AND (oid1 NOT IN
        (
            SELECT oid2
            FROM {xmatch_db}.{xmatch_table}
            WHERE oid1 < oid2
        ))
    )
) AS match ON match.oid2 = obs.oid
WHERE (obs.catflags = 0) AND (obs.magerr > 0)
GROUP BY match.oid1
HAVING length(lc) > 0
