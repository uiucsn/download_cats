import logging
import os
from urllib.parse import urljoin

from download_cats.base import BaseFetcher
from download_cats.utils import *


CURRENT_DR = 11


class ZtfDrLcFetcher(BaseFetcher):
    catalog_name = 'ZTF'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.dr = cli_args.dr
        self.base_url = f'https://irsa.ipac.caltech.edu/data/ZTF/lc_dr{self.dr}/'
        if self.dr >= 6:
            checksum_filename = 'checksum.md5'
        else:
            checksum_filename = 'checksums.md5'
        self.checksums_url = urljoin(self.base_url, checksum_filename)

    def __call__(self):
        logging.info(f'Fetching ZTF DR{self.dr} light curve data')
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
