from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


__all__ = ('ArgSubParser',)


class ArgSubParser(ABC):
    command = None

    def __init_subclass__(cls, **kwargs):
        if cls.command is None:
            raise NotImplementedError
        super().__init_subclass__(**kwargs)

    def __init__(self, cli_args: Namespace):
        self.cli_args = cli_args

    @abstractmethod
    def __call__(self):
        """Call putter"""
        pass

    @staticmethod
    @abstractmethod
    def add_arguments_to_parser(parser: ArgumentParser):
        """Add fetcher-specific arguments to argument parser"""
        # parser.add_argument('-e', '--example', help='example argument')
        pass
