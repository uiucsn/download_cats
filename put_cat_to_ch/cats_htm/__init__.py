from __future__ import annotations

import argparse
import logging
import os
import re
from collections import Counter
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import BinaryIO, Iterable, Tuple

import h5py
from joblib import delayed, Parallel, parallel_backend
from scipy.io import loadmat

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.cats_htm import sh, sql
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import remove_files_and_directory


__all__ = ('CatsHtmPutter', 'CatsHtmArgSubParser',)


class SingleCatHtm:
    dataset_name_re = re.compile(r'^htm_\d+$')

    def __init__(self, putter: CatsHtmPutter, path: str, name: str):
        self.putter = putter
        self.path = path
        self.name = name
        self.htm_col_cell_path = os.path.join(self.path, f'{self.name}_htmColCell.mat')
        self.htm_col_cell = loadmat(self.htm_col_cell_path)
        self.col_names = tuple(a[0] for a in self.htm_col_cell['ColCell'][0])
        self.col_units = tuple('dimensionless' if a.size == 0 else a[0] for a in self.htm_col_cell['ColUnits'][0])
        self.hdf5_paths = tuple(sorted(glob(os.path.join(self.path, '*.hdf5'))))
        
        self._check_columns(self.col_names, self.col_units)

    def _check_columns(self, names: Tuple[str], units: Tuple[str]):
        names_counter = Counter(s.lower() for s in names)
        if 'ra' not in names_counter:
            msg = f"Catalog {self.name} doesn't have column 'ra'"
            logging.error(msg)
            raise ValueError(msg)
        if 'dec' not in names_counter:
            msg = f"Catalog {self.name} doesn't have column 'dec'"
            logging.error(msg)
            raise ValueError(msg)
        if len(names) != len(names_counter):
            repeated = [k for k, v in names_counter.items() if v > 1]
            msg = f'Catalog {self.name} has some column repeated: {repeated}'
            logging.error(msg)
            raise NotImplementedError(msg)
        names_units = dict(zip(names, units))
        if names_units['ra'] != 'deg':
            msg = f'ra is in {names_units["ra"]}, only deg is supported'
            logging.error(msg)
            raise NotImplementedError(msg)
        if names_units['dec'] != 'deg':
            msg = f'dec is in {names_units["dec"]}, only deg is supported'
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

    def ch_columns_str(self) -> str:
        s = ',\n    '.join(f'{col.lower()} Float64' for col in self.ch_column_names)
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
        for path in self.hdf5_paths:
            with h5py.File(path, mode='r') as hdf5:
                for name, dataset in hdf5.items():
                    if not self.dataset_name_re.match(name):
                        continue
                    transposed = dataset[:].T.copy()
                    file.write(transposed)

    def __str__(self):
        return f'catsHTM catalog {self.name} located at {self.path}'


class CatsHtmPutter(CHPutter):
    db = 'htm'

    def __init__(self, dir, tmp_dir, user, host, clickhouse_settings, on_exists, cat, jobs, **_kwargs):
        self.data_dir = dir
        self.row_bin_dir = tmp_dir or self.data_dir
        self.catalogs = self._get_catalogs(dir, cat)
        self.processes = jobs
        self.on_exists = on_exists
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

    def _get_catalogs(self, dir: str, cat: Iterable[str]) -> Tuple[SingleCatHtm]:
        cat = list(cat)

        names = []
        for name in sorted(os.listdir(dir)):
            if name.startswith('.'):
                continue
            path = os.path.join(dir, name)
            if not os.path.isdir(path):
                continue
            if not any(f.endswith('.hdf5') for f in os.listdir(path)):
                continue
            names.append(name)

        cat_set = set(cat)
        if 'all' in cat_set:
            cat.remove('all')
            cat_set.remove('all')
            for name in names:
                if name not in cat_set:
                    cat.append(name)

        not_in_dir = cat_set - set(names)
        if not_in_dir:
            raise ValueError(f'catHTM catalogs are not found in {dir}: {not_in_dir}')

        logging.info(f'catsHTM catalogs to work with: {cat}')

        catalogs = tuple(SingleCatHtm(self, os.path.join(dir, c), c) for c in cat)
        return catalogs

    def create_tables(self, on_exists: str):
        logging.info('Creating DB tables for catsHTM')
        for c in self.catalogs:
            c.create_table(on_exists)

    def row_bin_paths(self) -> Tuple[str]:
        paths = tuple(os.path.join(self.row_bin_dir, f'{c.name}.dat') for c in self.catalogs)
        return paths

    def gen_row_bins_worker(self, path: str, c: SingleCatHtm):
        with open(path, 'wb') as f:
            c.write_row_binary(f)

    def gen_row_bins(self):
        os.makedirs(self.row_bin_dir, exist_ok=True)
        with parallel_backend('loky', n_jobs=self.processes):
            par = Parallel()
            worker = delayed(self.gen_row_bins_worker)
            par(worker(path, c) for path, c in zip(self.row_bin_paths(), self.catalogs))

    def insert_row_bins_worker(self, path: str, c: SingleCatHtm):
        self.shell_runner('insert_row_bin.sh', path, f'{c.db}.{c.table}', self.host)

    def insert_row_bins(self):
        with ThreadPool(processes=self.processes) as pool:
            pool.starmap(self.insert_row_bins_worker, zip(self.row_bin_paths(), self.catalogs))

    def remove_row_bins(self):
        logging.info(f'Removing CSV field files from {self.row_bin_dir}')
        remove_files_and_directory(self.row_bin_dir, self.row_bin_paths())

    default_actions = ('gen', 'create', 'insert', 'rm')

    def action_print_columns(self):
        from pprint import pprint

        logging.info('Printing catsHTM catalogs column information')
        for c in self.catalogs:
            print(c)
            pprint(list(zip(c.col_names, c.col_units)))

    def action_create(self):
        self.create_db(self.db)
        self.create_tables(self.on_exists)

    def action_gen(self):
        logging.info('Generating CH row binary data files from HDF5')
        self.gen_row_bins()

    def action_insert(self):
        logging.info('Inserting row binary files')
        self.insert_row_bins()

    def action_rm(self):
        logging.info('Removing row binary files')
        self.remove_row_bins()


class CatsHtmArgSubParser(ArgSubParser):
    command = 'htm'
    putter_cls = CatsHtmPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-c', '--cat', type=str.lower, default=('all',), nargs='+', help='catsHTM catalogs to use')
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs for "gen_row_bins" and "insert" actions')
