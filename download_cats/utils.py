import logging
import os
from hashlib import md5

import requests


DEFAULT_READ_CHUNK = 1 << 14
DEFAULT_DOWNLOAD_CHUNK = 1 << 14


def hash_file(path, chink_size=DEFAULT_READ_CHUNK):
    """Returns hex md5 checksum of file"""
    if not os.path.exists(path):
        logging.info(f"File {path} doesn't exist, md5 cannot be computed")
        return None
    logging.info(f'Computing md5 for {path}')
    with open(path, 'rb') as fh:
        m = md5()
        while True:
            chunk = fh.read(chink_size)
            if not chunk:
                break
            m.update(chunk)
    return m.digest().hex()


class FileDownloader:
    """Download URL content and safe to file optionally checking md5

    Arguments
    ---------
    url : str
        URL to download
    path : str
        Destination file path
    session : requests.Session or None
        requests.Session object to use, if None requests module is used instead
    checksum : str or None
        Hex representation of md5 checksum, if None no checksum validation
        performed

    Attributes
    ----------
    chunk_size : int
        Download chunk size
    """

    chunk_size = DEFAULT_DOWNLOAD_CHUNK

    def __init__(self, url, path, checksum=None, session=None):
        self.url = url
        self.path = path
        self.session = session or requests
        self.checksum = checksum
        if self.checksum is not None:
            self.md5 = md5()
            self.write = self._write_and_sum
        else:
            self.write = self._write

    def __enter__(self):
        self.fh = open(self.path, 'wb')
        self.resp = self.session.get(self.url, stream=True)
        self.resp.raise_for_status()
        for chunk in self.resp.iter_content(chunk_size=self.chunk_size):
            self.write(chunk)
        if self.checksum is not None and self.checksum != self.md5.digest().hex():
            raise ValueError('md5 checksum mismatch')

    def __exit__(self):
        self.fh.close()
        self.resp.close()

    def _write(self, chunk):
        self.fh.write(chunk)

    def _write_and_sum(self, chunk):
        self.fh.write(chunk)
        self.md5.update(chunk)


def download_file(url, path, checksum, session=None):
    """Download file and optionally checks its md5 checksum

    See call signature in `FileDownloader`

    Returns
    -------
    - True if file is downloaded
    - False if file exists and checksum matches
    """
    if os.path.exists(path):
        if checksum == hash_file(path):
            return False
    with FileDownloader(url, path, checksum=checksum, session=session):
        return True


def parse_checksums(s):
    """Extract filename-checksums pairs from checksums file content"""
    checksums = {}
    for line in s.splitlines():
        checksum, filename = line.split()
        checksums[filename] = checksum
    return checksums


def url_text_content(url, session=None):
    """String representation of URL content"""
    session = session or requests
    resp = session.get(url)
    resp.raise_for_status()
    return resp.text


__all__ = ('hash_file', 'parse_checksums', 'download_file', 'url_text_content')
