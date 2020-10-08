import logging
import os
from multiprocessing import Pool
from urllib.parse import urljoin

from download_cats.utils import *


BASE_URL = 'https://irsa.ipac.caltech.edu/data/ZTF/lc_dr3/'
CHECKSUMS_URL = urljoin(BASE_URL, 'checksums.md5')


def fetcher(args):
    logging.info(f'Fetching ZTF DR3 light curve data')
    checksums = parse_checksums(url_text_content(CHECKSUMS_URL))
    with Pool(processes=args.jobs) as pool:
        pool.starmap(
            download_file,
            ((urljoin(BASE_URL, fname), os.path.join(args.dir, fname), checksum)
             for fname, checksum in checksums.items()),
            chunksize=1
        )
