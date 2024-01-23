import logging
import os
from urllib.parse import urljoin

from download_cats.base import BaseFetcher
from download_cats.utils import *


CURRENT_DR = 11


class ZtfMetadataFetcher(BaseFetcher):
    catalog_name = 'ZTF_metadata'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.url = 'https://irsa.ipac.caltech.edu/data/ZTF/docs/ztf_metadata_latest.db'

    def __call__(self):
        logging.info(f'Fetching ZTF metadata database')
        download_file(self.url, os.path.join(self.dest, 'ztf_metadata_latest.db'))

    @staticmethod
    def add_arguments_to_parser(parser):
        pass


__all__ = ('ZtfMetadataFetcher',)
