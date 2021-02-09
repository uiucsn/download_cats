import logging
import os
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from download_cats.base import BaseFetcher
from download_cats.utils import *


class TwoMASS(BaseFetcher):
    catalog_name = '2MASS'

    base_url = 'https://irsa.ipac.caltech.edu/2MASS/download/allsky/'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir

    def _get_filenames(self):
        html = url_text_content(self.base_url)
        bs = BeautifulSoup(html)
        filenames = []
        for a in bs.find_all('a'):
            href = a['href']
            if not href.endswith('.gz'):
                continue
            filenames.append(href)
        return filenames

    def __call__(self):
        logging.info(f'Fetching 2MASS data')
        filenames = self._get_filenames()
        with process_pool(self.cli_args) as pool:
            pool.starmap(
                download_file,
                ((urljoin(self.base_url, fname), os.path.join(self.dest, fname)) for fname in filenames),
                chunksize=1
            )

    @staticmethod
    def add_arguments_to_parser(parser):
        pass
