import logging
import os
from urllib.parse import urljoin

from download_cats.base import BaseFetcher
from download_cats.utils import *


CURRENT_DR = 'dr3'


class ZtfDrLcFetcher(BaseFetcher):
    catalog_name = 'Gaia'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.dr = cli_args.dr
        self.base_url = f'http://cdn.gea.esac.esa.int/Gaia/g{self.dr}/gaia_source/'
        self.checksums_url = urljoin(self.base_url, '_MD5SUM.txt')

    def __call__(self):
        logging.info(f'Fetching Gaia {self.dr.upper()} light curve data')
        checksums = parse_checksums(url_text_content(self.checksums_url))
        with process_pool(self.cli_args) as pool:
            pool.starmap(
                download_file,
                ((urljoin(self.base_url, fname), os.path.join(self.dest, fname), checksum)
                 for fname, checksum in checksums.items() if fname.endswith('.csv.gz')),
                chunksize=1
            )

    @staticmethod
    def add_arguments_to_parser(parser):
        parser.add_argument('--dr', default=CURRENT_DR, type=str.lower,
                            help='Gaia data release version to download, like "dr3" or "edr3"')


__all__ = ('ZtfDrLcFetcher',)
