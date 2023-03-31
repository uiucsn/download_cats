import logging
import os
from urllib.parse import urljoin

from download_cats.base import BaseFetcher
from download_cats.utils import *


class Ps1StrmFetcher(BaseFetcher):
    catalog_name = 'Ps1-STRM'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.dr = cli_args.dr
        self.base_url = f'https://archive.stsci.edu/hlsps/ps1-strm/'
        self.checksums_url = urljoin(self.base_url, 'hlsp_ps1-strm_ps1_gpc1_all_multi_v1_md5sum.txt')

    def __call__(self):
        logging.info(f'Fetching PS1 STRM data')
        checksums = parse_checksums(url_text_content(self.checksums_url))
        with process_pool(self.cli_args) as pool:
            pool.starmap(
                download_file,
                ((urljoin(self.base_url, fname), os.path.join(self.dest, fname), checksum)
                 for fname, checksum in checksums.items() if fname.endswith('.csv.gz')),
                chunksize=1
            )


__all__ = ('Ps1StrmFetcher',)
