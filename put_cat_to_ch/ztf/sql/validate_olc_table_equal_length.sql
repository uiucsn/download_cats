SELECT sum(throwIf((ngoodobs != length(mjd)) OR (ngoodobs != length(mag) OR (ngoodobs != length(magerr)) OR (ngoodobs != length(clrcoeff))), 'arrays have different length from ngoodobs')) AS zero
FROM {db}.{table}
