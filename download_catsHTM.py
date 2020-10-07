#!/usr/bin/env python3

import logging
import os
from hashlib import md5
from multiprocessing import Pool
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from astropy.io import ascii
import requests


BASE_URL = 'https://euler1.weizmann.ac.il/catsHTM/'
HTML_TABLE_NAME = 'catsHTM_catalogs.html'
DATA_ROOT = '.'
PROCESSES = None


def get_catalog_list():
    url = urljoin(BASE_URL, HTML_TABLE_NAME)
    logging.info('Downloading catalog table HTML')
    table = ascii.read(url)
    table['wget_url'] = [urljoin(BASE_URL, file) for file in table['wget file']]
    table['checksum_url'] = [urljoin(BASE_URL, file) for file in table['checksum']]
    return table


class Downloader:
    read_chunk = 1 << 14
    download_chunk = 1 << 14

    def __init__(self, name, wget_url, checksum_url):
        self.name = name
        self.dest = os.path.join(DATA_ROOT, self.name)
        os.makedirs(self.dest, exist_ok=True)
        self.wget_url = wget_url
        self.checksum_url = checksum_url
        self.session = requests.Session()

    def text_content(self, url):
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.text

    def hash_file(self, path):
        if not os.path.exists(path):
            logging.info(f"{path} doesn't exist")
            return None
        logging.info(f'Computing md5 hash for {path}')
        m = md5()
        with open(path, 'rb') as fh:
            while True:
                part = fh.read(self.read_chunk)
                if not part:
                    break
                m.update(part)
        return m.digest().hex()

    def download_file(self, url, path):
        logging.info(f'Downloading {url} -> {path}')
        with self.session.get(url, stream=True) as resp:
            resp.raise_for_status()
            with open(path, 'wb') as fh:
                for chunk in resp.iter_content(chunk_size=self.download_chunk):
                    fh.write(chunk)

    def __call__(self):
        logging.info(f'Downloading {self.name}')

        wget_script = self.text_content(self.wget_url)
        urls = (line.split()[-1] for line in wget_script.splitlines())
        urls = {os.path.basename(url): url for url in urls}
        
        checksum_file = self.text_content(self.checksum_url)
        checksums = dict(line.split()[::-1] for line in checksum_file.splitlines())
        
        assert set(urls) == set(checksums)

        for filename, url in urls.items():
            path = os.path.join(self.dest, filename)
            if self.hash_file(path) == checksums[filename]:
                continue
            self.download_file(url, path)


def worker(*args, **kwargs):
    d = Downloader(*args, **kwargs)
    d()


def main():
    logging.basicConfig(level=logging.DEBUG)

    table = get_catalog_list()
    args = table[['Name', 'wget_url', 'checksum_url']]

    with Pool(processes=PROCESSES) as pool:
        pool.starmap(worker, args.iterrows(), chunksize=1)


if __name__ == '__main__':
    main()
