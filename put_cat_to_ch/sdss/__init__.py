import argparse
import glob
import logging
import os
from itertools import chain
from subprocess import PIPE

import numpy as np
from astropy.io import fits

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.sdss import sh, sql
from put_cat_to_ch.shell_runner import ShellRunner


DEFAULT_SDSS_DR = 16


SDSS_FILTERS = tuple('ugriz')


def np_dtype_to_ch(dtype):
    dtype = dtype.newbyteorder('<')
    if np.issubdtype(dtype, np.integer):
        n = 8 * dtype.itemsize
        if np.issubdtype(dtype, np.signedinteger):
            return f'Int{n}'
        return f'UInt{n}'
    if np.issubdtype(dtype, np.floating):
        n = 8 * dtype.itemsize
        return f'Float{n}'
    if np.issubdtype(dtype, np.bytes_):
        n = dtype.itemsize
        return f'FixedString({n})'
    raise ValueError(f"Don't know how to convert {dtype} to ClickHouse column type")


def np_dtype_field_to_ch(name, dtype):
    ch_name = name.lower()
    if np.issubdtype(dtype, np.void):
        sub_dtype, shape = dtype.subdtype
        assert shape == (len(SDSS_FILTERS),)
        return sum((np_dtype_field_to_ch(f'{ch_name}_{fltr}', sub_dtype) for fltr in SDSS_FILTERS), [])
    ch_type = np_dtype_to_ch(dtype)
    return [(ch_name, ch_type)]


class SDSSPutter(CHPutter):
    """Put SDSS photoObj table to clickhouse

    Parameters
    ----------
    dr: str
        Folder which subfolders contains photoObj-*.fits
    """
    db = 'sdss'

    def __init__(self, dir, user, host, clickhouse_settings, on_exists, jobs, dr, **_kwargs):
        self.data_dir = dir
        self.fits_glob_pattern = os.path.join(dir, '**/calibObj-*-star.fits.gz')
        self.fits_dtype = self._get_fits_data_dtype(self.fits_glob_pattern)
        self.le_dtype = np.dtype([(name, dt.newbyteorder('<'))
                                  for name, (dt, _offset) in self.fits_dtype.fields.items()])
        self.processes = jobs
        self.on_exists = on_exists
        self.dr = dr
        self.user = user
        self.host = host
        self.settings = clickhouse_settings
        super().__init__(
            sql,
            host=self.host,
            database=self.db,
            user=self.user,
            settings=self.settings,
            connect_timeout=86400,
            send_receive_timeout=86400,
            sync_request_timeout=86400,
        )
        self.shell_runner = ShellRunner(sh)

    @staticmethod
    def _get_fits_data_dtype(glob_pattern):
        path = next(glob.iglob(glob_pattern, recursive=True))
        data = fits.getdata(path, memmap=False)
        return data.dtype

    @property
    def table_name(self):
        return f'dr{self.dr}_calibObj'

    @property
    def ch_columns(self):
        return dict(chain.from_iterable(np_dtype_field_to_ch(name, dtype)
                                        for name, (dtype, _offset) in self.le_dtype.fields.items()))

    @property
    def ch_columns_str(self):
        return f',\n    '.join(f'{name} {ch_type}' for name, ch_type in self.ch_columns.items())

    def create_table(self, on_exists: str):
        exists_ok = self.process_on_exists(on_exists, self.db, self.table_name)
        self.exe_query(
            'create_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.table_name,
            columns=self.ch_columns_str,
        )

    def insert_fits(self, path, proc):
        logging.info(f'Inserting {path}')
        data = fits.getdata(path, memmap=False)
        data = np.asarray(data, dtype=self.le_dtype)
        proc.communicate(data.tobytes())

    def insert_data(self):
        logging.info('Collecting FITS paths')
        paths = sorted(glob.iglob(self.fits_glob_pattern, recursive=True))
        logging.info('Starting shell insert script')
        with self.shell_runner.popen(
                'insert.sh',
                f'{self.db}.{self.table_name}',
                self.host,
                stdin=PIPE,
                text=False,
        ) as proc:
            for path in paths:
                self.insert_fits(path, proc)

    default_actions = ('create', 'insert',)

    def action_create(self):
        self.create_db(self.db)
        self.create_table(self.on_exists)

    def action_insert(self):
        logging.info('Inserting fits files')
        self.insert_data()


class SDSSArgSubParser(ArgSubParser):
    command = 'sdss'
    putter_cls = SDSSPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs for "insert" action')
        parser.add_argument('--dr', type=int, default=DEFAULT_SDSS_DR,
                            help='SDSS DR number')
