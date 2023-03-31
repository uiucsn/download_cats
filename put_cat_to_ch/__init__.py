from . import arg_sub_parser, utils
from .cats_htm import *
from .des import *
from .dust_maps import *
from .gaia_dr import *
from .ps1 import *
from .ps1_strm import *
from .sdss import *
from .twomass import *
from .ztf import *


ARG_SUB_PARSERS = {cls.command: cls for cls in utils.subclasses(arg_sub_parser.ArgSubParser)}
