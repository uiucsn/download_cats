from __future__ import annotations

import argparse
import logging
import os
from functools import cached_property
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import astropy.io.ascii
import astropy.table
from download_cats.gaia_dr import CURRENT_DR as CURRENT_GAIA_DR

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.gaia_dr import sh, sql
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import remove_files_and_directory, np_dtype_to_ch

__all__ = ('GaiaDrPutter', 'GaiaDrArgSubParser',)


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
        return astropy.io.ascii.read(first_file, format='ecsv', fill_values=('null', '0'))

    @property
    def ch_columns(self) -> Dict[str, str]:
        return {column.name: np_dtype_to_ch(column.dtype, nullable=hasattr(column, 'mask'))
                for column in self.first_table.itercols()}

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
        self.shell_runner('insert.sh', file, f'{self.db}.{self.table_name}', self.host)

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
        parser.add_argument('--dr', default=CURRENT_GAIA_DR, help='Gaia ZTF DR like "dr3" or "edr3"')
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs "insert" actions')
