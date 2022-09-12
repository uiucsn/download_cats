from __future__ import annotations

import argparse
import logging
import os
import re
from collections import Counter
from functools import lru_cache, cached_property
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import BinaryIO, Iterable, List, Tuple

import astropy.table
import h5py
import numpy as np
from astropy.ascii import read
from download_cats.cats_htm import get_catalog_list
from download_cats.gaia_dr import CURRENT_DR as CURRENT_GAIA_DR
from joblib import delayed, Parallel, parallel_backend
from scipy.io import loadmat

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.gaia_dr import sh, sql
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import remove_files_and_directory, np_dtype_field_to_ch

__all__ = ('GaiaDrPutter', 'GaiaDrArgSubParser',)


class SingleCatHtm:
    def __init__(self, putter: CatsHtmPutter, path: str, name: str):
        logging.info(f'Constructing SingleCatHtm instance for {name}')

        self.putter = putter
        self.path = path
        self.name = name
        self.htm_col_cell_path = os.path.join(self.path, f'{self.name}_htmColCell.mat')
        self.htm_col_cell = loadmat(self.htm_col_cell_path)
        self.col_names = tuple(np.concatenate(self.htm_col_cell['ColCell'].flatten()))
        self.col_units = tuple('dimensionless' if a.size == 0 else a[0] for a in self.htm_col_cell['ColUnits'].flat)
        self.hdf5_paths = tuple(sorted(glob(os.path.join(self.path, '*.hdf5'))))

        self._check_columns(self._prepare_column_names_for_ch(), self.col_units)

    def _check_columns(self, ch_names: Tuple[str], units: Tuple[str]):
        names_counter = Counter(ch_names)
        if len(ch_names) != len(names_counter):
            repeated = [k for k, v in names_counter.items() if v > 1]
            msg = f'Catalog {self.name} has some column repeated: {repeated}'
            logging.error(msg)
            raise NotImplementedError(msg)

    @property
    def db(self):
        return self.putter.db

    @property
    def table(self):
        return self.name

    @property
    def ch_column_names(self) -> Tuple[str]:
        return tuple(col.lower() for col in self.col_names)

    def _prepare_column_names_for_ch(self) -> List[str]:
        cols = list(self.ch_column_names)
        logging.info(f'cols = {cols}')
        if self.name == 'AKARI':
            cols[cols.index('ra')] = 'ra_rad'
            cols[cols.index('dec')] = 'dec_rad'
            cols[cols.index('ra')] = 'ra_arcsec'
            cols[cols.index('dec')] = 'dec_arcsec'
        elif self.name == 'HSCv2':
            cols[cols.index('matchra')] = 'ra_rad'
            cols[cols.index('matchdec')] = 'dec_rad'
        else:
            cols[cols.index('ra')] = 'ra_rad'
            cols[cols.index('dec')] = 'dec_rad'
        return cols

    def ch_columns_str(self) -> str:
        s = ',\n    '.join(f'`{col.lower()}` Float64' for col in self._prepare_column_names_for_ch())
        return s

    def create_table(self, on_exists: str = 'fail'):
        exists_ok = self.putter.process_on_exists(on_exists, self.db, self.table)
        self.putter.exe_query(
            'create_table.sql',
            if_not_exists=self.putter.if_not_exists(exists_ok),
            db=self.db,
            table=self.table,
            columns=self.ch_columns_str(),
        )

    def write_row_binary(self, file: BinaryIO):
        logging.info(f'Writing HDF5 data into {file}')
        for path in self.hdf5_paths:
            logging.info(f'Writing HDF5 file {path} into {file}')
            with h5py.File(path, mode='r') as hdf5:
                for name, dataset in hdf5.items():
                    if not self.dataset_name_re.match(name):
                        continue
                    transposed = dataset[:].T.copy()
                    file.write(transposed)

    def __str__(self):
        return f'catsHTM catalog {self.name} located at {self.path}'


class GaiaDrPutter(CHPutter):
    def __init__(self, dir, user, host, clickhouse_settings, on_exists, jobs, dr, **_kwargs):
        self.data_dir = dir
        self.processes = jobs
        self.on_exists = on_exists
        self.user = user
        self.host = host
        self.settings = clickhouse_settings
        self.dr = dr
        self.db = f'gaia_{self.dr}'
        self.table_name = 'gaia_source'
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

    @cached_property
    def input_files(self) -> List[str]:
        return sorted(glob(os.path.join(self.data_dir, 'GaiaSource*.csv.gz')))

    @cached_property
    def first_table(self) -> astropy.table.Table:
        first_file = self.input_files[0]
        logging.info(f'Getting columns from {first_file}')
        return read(first_file, format='ecsv', fill_values=('null', '0'))

    @property
    def ch_columns(self) -> Dict[str, str]:
        return dict(np_dtype_field_to_ch(column.name, column.dtype, nullable=hasattr(column, 'mask'))
                    for column in self.first_table.itercols())

    @property
    def ch_columns_str(self) -> str:
        return f',\n    '.join(f'{name} {ch_type}' for name, ch_type in self.ch_columns.items())

    def create_table(self, on_exists: str):
        logging.info('Creating DB table for Gaia DR')
        exists_ok = self.process_on_exists(on_exists, self.db, self.table_name)
        self.exe_query(
            'create_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.table_name,
            columns=self.ch_columns_str,
        )

    def insert_single_file(self, file: str):
        logging.info(f'Inserting {file} into {self.db}.{self.table_name}')
        self.shell_runner.run('insert.sh', file, f'{self.db}.{self.table_name}', self.host)

    def insert(self):
        with ThreadPool(processes=self.processes) as pool:
            pool.map(self.insert_single_file, self.input_files)

    default_actions = ('create', 'insert')

    def action_create(self):
        self.create_db(self.db)
        self.create_table(self.on_exists)

    def action_insert(self):
        logging.info('Inserting row binary files')
        self.insert()


class GaiaDrArgSubParser(ArgSubParser):
    command = 'gaia'
    putter_cls = GaiaDrPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('--dr', default=CURRENT_GAIA_DR, type=int, help='Gaia ZTF DR like "dr3" or "edr3"')
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs "insert" actions')
