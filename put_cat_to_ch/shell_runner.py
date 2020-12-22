import logging
from importlib.resources import read_text
from subprocess import check_call
from types import ModuleType


class ShellRunner:
    def __init__(self, module: ModuleType):
        self.module = module

    def __call__(self, filename: str, *args: str) -> int:
        script = read_text(self.module, filename)
        args = ('sh', '-c', script, filename) + args
        logging.info(f'Executing {" ".join(args)}')
        return check_call(args)
