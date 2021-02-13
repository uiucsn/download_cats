SELECT count() as rows,
    sum(pts_key) as sum_pts_key,
    sum(pxcntr) as sum_pxcntr,
    sum(scan_key) as sum_scan_key,
    sum(scan) as sum_scan,
    sum(ext_key) as sum_ext_key,
    sum(coadd_key) as sum_coadd_key,
    sum(coadd) as sum_coadd,
    sum(mp_flg) as sum_mp_flg,
    sum(gal_contam) as sum_gal_contam,
    sum(use_src) as sum_use_src,
    sum(dup_src) as sum_dup_src,
    sum(nopt_mchs) as sum_nopt_mchs,
    sum(phi_opt) as sum_phi_opt,
    sum(dist_edge_ew) as sum_dist_edge_ew,
    sum(dist_edge_ns) as sum_dist_edge_ns,
    sum(err_ang) as sum_err_ang
FROM {db}.{table};
