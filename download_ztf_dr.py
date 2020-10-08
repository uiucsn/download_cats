#!/usr/bin/env python3

import logging
import os
from hashlib import md5
from itertools import tee
from multiprocessing import Pool
from urllib.parse import urljoin

import requests


DATA_ROOT = '.'
BASE_URL = 'https://irsa.ipac.caltech.edu/data/ZTF/lc_dr3/'
CHECKSUMS_URL = urljoin(BASE_URL, 'checksums.md5')

DOWNLOAD_CHUNK = 1 << 14
DEFAULT_READ_CHUNK = 1 << 14
PROCESSES = 8


def hash_file(path, chink_size=DEFAULT_READ_CHUNK):
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as fh:
        m = md5()
        while True:
            chunk = fh.read(chink_size)
            if not chunk:
                break
            m.update(chunk)
    return m.digest().hex()


def download(url, path, checksum):
    if checksum == hash_file(path):
        logging.info(f'File {path} exists and checksum is right')
        return
    logging.info(f'Downloading {url} to {path}')
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(path, 'wb') as fh:
            m = md5()
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK):
                fh.write(chunk)
                m.update(chunk)
    assert checksum == m.digest().hex(), 'checksum is wrong'


def get_checksums(url):
    logging.info(f'Downloading {url}')
    resp = requests.get(url)
    resp.raise_for_status()
    checksums = dict(line.split()[::-1] for line in resp.text.splitlines())
    return checksums


def main():
    logging.basicConfig(level=logging.INFO)

    checksums = get_checksums(CHECKSUMS_URL)
    with Pool(processes=PROCESSES) as pool:
        pool.starmap(
            download,
            ((urljoin(BASE_URL, fname), os.path.join(DATA_ROOT, fname), checksum)
             for fname, checksum in checksums.items()),
            chunksize=1
        )


if __name__ == '__main__':
    main()
