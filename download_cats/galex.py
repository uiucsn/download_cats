import logging
import os
from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup

from download_cats.base import BaseFetcher
from download_cats.utils import *


class GalexFetcher(BaseFetcher):
    catalog_name = 'GALEX'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs
        self.base_url = 'http://dolomiti.pha.jhu.edu/uvsky/GUVcat/GUVcat_AIS_FOV055/2019/5deglatslices/'
        self.content_list_url = 'http://dolomiti.pha.jhu.edu/uvsky/GUVcat/GUVcat_AIS.html'

    def _get_urls_filenames(self):
        logging.info(f'Getting index of GALEX catalogs of unique UV sources')
        html = url_text_content(self.content_list_url)
        bs = BeautifulSoup(html)
        urls = []
        filenames = []
        for a in bs.find_all('a'):
            href = a['href']
            if not href.startswith(self.base_url):
                continue
            if not href.endswith('.fits.gz'):
                continue
            if href.endswith('FUV.fits.gz'):
                continue
            urls.append(href)
            *_, filename = href.split('/')
            filenames.append(filename)
        return urls, filenames

    def __call__(self):
        logging.info(f'Fetching GALEX catalogs of unique UV sources')
        urls, filenames = self._get_urls_filenames()
        paths = [os.path.join(self.dest, fname) for fname in filenames]
        with process_pool(self.cli_args) as pool:
            pool.starmap(download_file, zip(urls, paths), chunksize=1)

    @staticmethod
    def add_arguments_to_parser(parser):
        pass


__all__ = ('GalexFetcher',)
