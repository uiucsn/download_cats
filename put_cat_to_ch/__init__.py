from . import arg_sub_parser, utils
from .ztf import ZtfPutter


ARG_SUB_PARSERS = {cls.command: cls for cls in utils.subclasses(arg_sub_parser.ArgSubParser)}
