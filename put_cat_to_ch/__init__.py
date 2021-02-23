from . import arg_sub_parser, utils
from .cats_htm import *
from .dust_maps import *
from .sdss import *
from .twomass import *
from .ztf import *


ARG_SUB_PARSERS = {cls.command: cls for cls in utils.subclasses(arg_sub_parser.ArgSubParser)}
