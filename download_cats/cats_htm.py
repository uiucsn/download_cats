import logging
import os
from multiprocessing import Pool
from urllib.parse import urljoin

from astropy.io import ascii
import requests

from download_cats.utils import *


BASE_URL = 'https://euler1.weizmann.ac.il/catsHTM/'
HTML_TABLE_NAME = 'catsHTM_catalogs.html'


def get_catalog_list(dest):
    url = urljoin(BASE_URL, HTML_TABLE_NAME)
    logging.info('Downloading catalog table HTML')
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
        download_file(url, path, checksum=checksums[filename], session=session)


def fetcher(cli_args):
    logging.info(f'Fetching catsHTM catalogues')
    table = get_catalog_list(cli_args.dir)
    args = table[['Name', 'dest', 'wget_url', 'checksum_url']]

    with Pool(processes=cli_args.jobs) as pool:
        pool.starmap(download_catalog, args.iterrows(), chunksize=1)


__all__ = ('fetcher',)
