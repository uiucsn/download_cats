import logging
import os
from pathlib import Path
from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup

from download_cats.base import BaseFetcher
from download_cats.utils import *


CURRENT_DR = 2


class DesFetcher(BaseFetcher):
    catalog_name = 'DES'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.dr = cli_args.dr
        self.base_url = f'https://desdr-server.ncsa.illinois.edu/despublic/dr{self.dr}_tiles/'

    def _get_urls_filenames(self):
        logging.info(f'Getting index of DES DR{self.dr} tiles')
        html = url_text_content(self.base_url)
        bs = BeautifulSoup(html)
        urls = []
        filenames = []
        for td in bs.find_all('td'):
            if (a := td.a) is None:
                continue
            if (href := a['href']) is None:
                continue
            name = Path(href).name
            if not name.startswith('DES'):
                continue
            filename = f'{unquote(name)}_dr2_main.fits'
            urls.append(urljoin(self.base_url, f'{name}/{filename}'))
            filenames.append(filename)
        return urls, filenames

    def __call__(self):
        logging.info(f'Fetching DES DR{self.dr} main table')
        urls, filenames = self._get_urls_filenames()
        assert len(urls) > 0
        paths = [os.path.join(self.dest, fname) for fname in filenames]
        with process_pool(self.cli_args) as pool:
            pool.starmap(download_file, zip(urls, paths), chunksize=1)

    @staticmethod
    def add_arguments_to_parser(parser):
        parser.add_argument('--dr', default=CURRENT_DR, type=int,
                            help='DES data release version to download')


__all__ = ('DesFetcher',)
