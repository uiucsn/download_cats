from . import base, cats_htm, ztf_dr_lc, utils


FETCHERS = {cls.catalog_name.lower(): cls for cls in utils.subclasses(base.BaseFetcher)}
