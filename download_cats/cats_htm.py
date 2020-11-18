import logging
import os
from multiprocessing import Pool
from urllib.parse import urljoin

from astropy.io import ascii
import requests

from download_cats.base import BaseFetcher
from download_cats.utils import *


BASE_URL = 'https://euler1.weizmann.ac.il/catsHTM/'
HTML_TABLE_NAME = 'catsHTM_catalogs.html'


def get_catalog_list(dest):
    url = urljoin(BASE_URL, HTML_TABLE_NAME)
    logging.info('Downloading catalog HTML table')
    table = ascii.read(url, format='html')
    table['dest'] = [os.path.join(dest, name) for name in table['Name']]
    table['wget_url'] = [urljoin(BASE_URL, file) for file in table['wget file']]
    table['checksum_url'] = [urljoin(BASE_URL, file) for file in table['checksum']]
    return table


def download_catalog(name, dest, wget_url, checksum_url):
    logging.info(f'Downloading {name}')

    os.makedirs(dest, exist_ok=True)
    session = requests.Session()

    wget_script = url_text_content(wget_url, session)
    urls = (line.split()[-1] for line in wget_script.splitlines())
    urls = {os.path.basename(url): url for url in urls}

    checksums = parse_checksums(url_text_content(checksum_url, session))

    assert set(urls) == set(checksums)

    for filename, url in urls.items():
        path = os.path.join(dest, filename)
        download_file(url, path, checksum=checksums[filename], session=session, retries=3)


class CatsHTMFetcher(BaseFetcher):
    catalog_name = 'HTM'

    def __init__(self, cli_args):
        super().__init__(cli_args)
        self.dest = cli_args.dir
        self.processes = cli_args.jobs

        self.catalogs = frozenset(cli_args.cat)
        if 'all' in self.catalogs:
            self.catalogs = Everything()

    def __call__(self):
        logging.info(f'Fetching catsHTM catalogues')
        table = get_catalog_list(self.dest)
        args = table[['Name', 'dest', 'wget_url', 'checksum_url']]

        with process_pool(self.cli_args) as pool:
            pool.starmap(
                download_catalog,
                (x for x in args.iterrows() if x[0].lower() in self.catalogs),
                chunksize=1,
            )

    @staticmethod
    def add_arguments_to_parser(parser):
        parser.add_argument('-c', '--cat', default=['all'], type=str.lower, nargs='+',
                            help=('HTM catalogs to download, "All" (default) means download all catalogs, see full '
                                  'list on the catsHTM homepage: '
                                  'https://euler1.weizmann.ac.il/catsHTM/catsHTM_catalogs.html'))


__all__ = ('CatsHTMFetcher',)
