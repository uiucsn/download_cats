from . import base, cats_htm, gaia_dr, twomass, ztf_dr_lc, utils


FETCHERS = {cls.catalog_name.lower(): cls for cls in utils.subclasses(base.BaseFetcher)}
