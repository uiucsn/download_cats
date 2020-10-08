from abc import ABC, abstractmethod
from argparse import ArgumentParser


class BaseFetcher(ABC):
    catalog_name = None

    def __init_subclass__(cls, **kwargs):
        if cls.catalog_name is None:
            raise NotImplementedError
        super().__init_subclass__(**kwargs)

    @abstractmethod
    def __init__(self, cli_args):
        self.cli_args = cli_args

    @abstractmethod
    def __call__(self):
        """Actually fetch data"""
        pass

    @staticmethod
    @abstractmethod
    def add_arguments_to_parser(parser: ArgumentParser):
        """Add fetcher-specific arguments to argument parser"""
        # parser.add_argument('-e', '--example', help='example argument')
        pass
