import logging
import os
from hashlib import md5
from multiprocessing import Pool
from time import sleep

import requests


DEFAULT_READ_CHUNK = 1 << 14
DEFAULT_DOWNLOAD_CHUNK = 1 << 14


class HashSumCheckFailed(RuntimeError):
    pass


def hash_file(path, chunk_size=DEFAULT_READ_CHUNK):
    """Returns hex md5 checksum of file"""
    if not os.path.exists(path):
        logging.info(f"File {path} doesn't exist, md5 cannot be computed")
        return None
    logging.info(f'Computing md5 for {path}')
    with open(path, 'rb') as fh:
        m = md5()
        while True:
            chunk = fh.read(chunk_size)
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

    def __init__(self, url, path, checksum=None, session=None, retries=1):
        self.url = url
        self.path = path
        self.session = session or requests
        assert retries > 0
        self.retries = retries
        self.checksum = checksum
        if self.checksum is not None:
            self.md5 = md5()
            self.write = self._write_and_sum
        else:
            self.write = self._write

    def _create_dir(self):
        abspath = os.path.abspath(self.path)
        dirpath = os.path.dirname(abspath)
        logging.info(f"Creating {dirpath} directory if it doesn't exist")
        os.makedirs(dirpath, exist_ok=True)

    def download(self):
        logging.info(f'Downloading {self.url} to {self.path}')
        self._create_dir()
        self.fh = open(self.path, 'wb')
        self.resp = self.session.get(self.url, stream=True)
        self.resp.raise_for_status()
        for chunk in self.resp.iter_content(chunk_size=self.chunk_size):
            self.write(chunk)
        if self.checksum is not None and self.checksum != self.md5.digest().hex():
            msg = f'md5 checksum mismatch for {self.url}'
            logging.warning(msg)
            raise HashSumCheckFailed(msg)

    def __enter__(self):
        for _ in range(self.retries):
            try:
                return self.download()
            except HashSumCheckFailed as e:
                exception = e
            except requests.exceptions.RequestException as e:
                exception = e
                sleep(1)
        raise exception

    def __exit__(self, exc_type, exc_value, traceback):
        self.fh.close()
        self.resp.close()

    def _write(self, chunk):
        self.fh.write(chunk)

    def _write_and_sum(self, chunk):
        self.fh.write(chunk)
        self.md5.update(chunk)


def download_file(url, path, checksum=None, session=None, retries=1):
    """Download file and optionally checks its md5 checksum

    See call signature in `FileDownloader`

    Returns
    -------
    - True if file is downloaded
    - False if file exists and checksum matches
    """
    if os.path.exists(path):
        if checksum is not None and checksum == hash_file(path):
            logging.info(f'File {path} exists and checksum matches')
            return False
    with FileDownloader(url, path, checksum=checksum, session=session, retries=retries):
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


def subclasses(cls):
    return set(cls.__subclasses__()).union(subcls for c in cls.__subclasses__() for subcls in subclasses(c))


class Everything:
    def __contains__(self, value):
        return True

    def __repr__(self):
        return 'Everything'


def configure_logging(cli_args):
    if cli_args.verbose == 0:
        logging_level = logging.ERROR
    elif cli_args.verbose == 1:
        logging_level = logging.WARNING
    elif cli_args.verbose == 2:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    logging.basicConfig(level=logging_level)


def pool_initializer(cli_args):
    configure_logging(cli_args)


def process_pool(cli_args, **kwargs):
    return Pool(
        processes=cli_args.jobs,
        initializer=pool_initializer,
        initargs=(cli_args,),
        **kwargs
    )


__all__ = ('hash_file', 'parse_checksums', 'download_file', 'url_text_content', 'Everything', 'configure_logging',
           'process_pool',)
