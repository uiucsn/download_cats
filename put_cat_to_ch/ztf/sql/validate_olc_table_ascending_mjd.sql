SELECT sum(throwIf(countEqual(arrayDifference(mjd), 0) != 1, 'mjd is not in ascending order')) AS zero
FROM {db}.{table}
