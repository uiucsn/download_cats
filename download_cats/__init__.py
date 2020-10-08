from . import cats_htm, ztf_dr_lc

FETCHERS = {
    'catshtm': cats_htm.fetcher,
    'ztf': ztf_dr_lc.fetcher,
}
