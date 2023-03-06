import argparse
import glob
import logging
import os
from itertools import chain
from multiprocessing import Pool
from subprocess import PIPE

import numpy as np
from astropy.io import fits

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.des import sh, sql
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import np_dtype_to_ch, dtype_to_le


DEFAULT_DES_DR = 2

class DESPutter(CHPutter):
    """Put DES main table to clickhouse
    """
    db = 'des'

    def __init__(self, dir, user, host, clickhouse_settings, on_exists, jobs, dr, **_kwargs):
        self.data_dir = dir
        self.fits_glob_pattern = os.path.join(dir, '*fits')
        self.fits_dtype = self._get_fits_data_dtype(self.fits_glob_pattern)
        self.le_dtype = dtype_to_le(self.fits_dtype)
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
        path = next(glob.iglob(glob_pattern))
        data = fits.getdata(path, memmap=False)
        return data.dtype

    @property
    def table_name(self):
        return f'dr{self.dr}_calibObj'

    @property
    def ch_columns(self):
        return dict(chain.from_iterable(np_dtype_to_ch(dtype, str_is_bytes=True)
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

    def insert_fits_file(self, path):
        logging.info(f'Inserting {path}')
        data = fits.getdata(path, memmap=False)
        data = np.asarray(data, dtype=self.le_dtype)
        with self.shell_runner.popen(
                'insert.sh',
                f'{self.db}.{self.table_name}',
                self.host,
                stdin=PIPE,
                text=False,
        ) as proc:
            proc.stdin.write(data)

    def insert_data(self):
        logging.info('Collecting FITS paths')
        paths = sorted(glob.iglob(self.fits_glob_pattern, recursive=True))
        with Pool(self.processes) as pool:
            pool.map(self.insert_fits_file, paths, chunksize=min(128, len(paths)//self.processes))

    default_actions = ('create', 'insert',)

    def action_create(self):
        self.create_db(self.db)
        self.create_table(self.on_exists)

    def action_insert(self):
        logging.info('Inserting fits files')
        self.insert_data()


class DESArgSubParser(ArgSubParser):
    command = 'des'
    putter_cls = DESPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs for "insert" action')
        parser.add_argument('--dr', type=int, default=DEFAULT_DES_DR,
                            help='DES DR number')
