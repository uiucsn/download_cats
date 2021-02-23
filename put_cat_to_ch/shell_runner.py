import logging
from importlib.resources import read_text
from subprocess import check_call, Popen
from types import ModuleType
from typing import Tuple


class ShellRunner:
    def __init__(self, module: ModuleType):
        self.module = module

    def _args(self, filename: str, *args: str) -> Tuple[str]:
        script = read_text(self.module, filename)
        args = ('sh', '-c', script, filename) + args
        return args

    def __call__(self, filename: str, *args: str) -> int:
        args = self._args(filename, *args)
        logging.info(f'Executing {" ".join(args)}')
        return check_call(args)

    def popen(self, filename: str, *args: str, **kwargs):
        args = self._args(filename, *args)
        logging.info(f'Executing {" ".join(args)}')
        return Popen(args, **kwargs)
