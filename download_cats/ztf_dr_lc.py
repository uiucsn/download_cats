import logging
import os
from multiprocessing import Pool
from urllib.parse import urljoin

from download_cats.base import BaseFetcher
from download_cats.utils import *


CURRENT_DR = 4


class ZtfDrLcFetcher(BaseFetcher):
    catalog_name = 'ZTF'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.dr = cli_args.dr
        self.base_url = f'https://irsa.ipac.caltech.edu/data/ZTF/lc_dr{self.dr}/'
        self.checksums_url = urljoin(self.base_url, 'checksums.md5')

    def __call__(self):
        logging.info(f'Fetching ZTF DR3 light curve data')
        checksums = parse_checksums(url_text_content(self.checksums_url))
        with process_pool(self.cli_args) as pool:
            pool.starmap(
                download_file,
                ((urljoin(self.base_url, fname), os.path.join(self.dest, fname), checksum)
                 for fname, checksum in checksums.items()),
                chunksize=1
            )

    @staticmethod
    def add_arguments_to_parser(parser):
        parser.add_argument('--dr', default=CURRENT_DR, type=int,
                            help='ZTF data release version to download')


__all__ = ('ZtfDrLcFetcher',)
