#!/usr/bin/env python3

import logging
import os
from multiprocessing import Pool
from subprocess import check_call
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from astropy.io import ascii
import requests


BASE_URL = 'https://euler1.weizmann.ac.il/catsHTM/'
HTML_TABLE_NAME = 'catsHTM_catalogs.html'
DATA_ROOT = '.'
PROCESSES = 32


def get_catalog_list():
    url = urljoin(BASE_URL, HTML_TABLE_NAME)
    logging.info('Downloading catalog table HTML')
    table = ascii.read(url)
    table['wget_url'] = [urljoin(BASE_URL, file) for file in table['wget file']]
    table['checksum_url'] = [urljoin(BASE_URL, file) for file in table['checksum']]
    return table


def download_to_file(url, file):
    logging.info(f'Downloading {url} -> {file.name}')
    resp = requests.get(url)
    resp.raise_for_status()
    file.write(resp.content)
    file.flush()


def download_catalog(name, wget_url, checksum_url):
    logging.info(f'Downloading catalog {name}')
    with NamedTemporaryFile() as script, NamedTemporaryFile() as checksums:
        download_to_file(wget_url, script)
        download_to_file(checksum_url, checksums)
        check_call(['sh', script.name], cwd=DATA_ROOT)
        path = os.path.join(DATA_ROOT, name)
        check_call(['md5sum', '-c', checksums.name], cwd=path)


def main():
    logging.basicConfig(level=logging.DEBUG)

    table = get_catalog_list()
    args = table[['Name', 'wget_url', 'checksum_url']]

    with Pool(processes=PROCESSES) as pool:
        pool.starmap(download_catalog, args.iterrows(), chunksize=1)


if __name__ == '__main__':
    main()
