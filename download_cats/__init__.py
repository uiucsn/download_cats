from . import base, cats_htm, des, gaia_dr, galex, ps1_strm, twomass, ztf_dr_lc, ztf_metadata, utils


FETCHERS = {cls.catalog_name.lower(): cls for cls in utils.subclasses(base.BaseFetcher)}
