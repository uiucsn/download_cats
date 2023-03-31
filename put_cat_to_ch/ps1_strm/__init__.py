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

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.ps1_strm import sh, sql
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import np_dtype_to_ch

__all__ = ('Ps1StrmPutter', 'Ps1StrmArgSubParser',)


# Readme specifies weird types, do not use it for other catalogs
def _sql_type_to_ch(sql):
    match sql:
        case 'bigint':
            return 'Int64'
        case 'float':
            return 'Float64'
        # we know that it is class name
        case 'varchar[8]':
            return "Enum('GALAXY', 'STAR', 'QSO', 'UNSURE')"
        case 'int':
            return 'Int32'
        case other:
            raise ValueError(f'SQL type {other} is not supported')



class Ps1StrmPutter(CHPutter):
    # Put to the same DB as the main PS1 tables
    db = 'ps1'

    def __init__(self, dir, user, host, clickhouse_settings, on_exists, jobs, **_kwargs):
        self.data_dir = dir
        self.processes = jobs
        self.on_exists = on_exists
        self.user = user
        self.host = host
        self.settings = clickhouse_settings
        self.table_name = 'strm'
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
        files = sorted(glob(os.path.join(self.data_dir, 'hlsp_ps1-strm_ps1_gpc1*.csv.gz')))
        assert len(files) > 0, f'No files found in {self.data_dir}'
        return files

    @cached_property
    def column_definitions_from_readme(self) -> Dict[str, str]:
        with open(os.path.join(self.data_dir, 'hlsp_ps1-strm_ps1_gpc1_all_multi_v1_readme.txt')) as fh:
            # Read until footer
            for line in fh:
                if line.startswith('Column Name'):
                    break
            assert fh.readline().startswith('-------')

            names = []
            types = []
            for line in fh:
                line = line.strip()
                if line == '':
                    continue
                name, type, *_ = line.split()
                names.append(name)
                types.append(types)
        return dict(zip(names, types))

    @property
    def ch_columns(self) -> Dict[str, str]:
        return {name: _sql_type_to_ch(type) for name, type in self.column_definitions_from_readme.items()}

    @property
    def ch_columns_str(self) -> str:
        return f',\n    '.join(f'{name} {ch_type}' for name, ch_type in self.ch_columns.items())

    def create_table(self, on_exists: str):
        logging.info('Creating DB table for PS1-STRM catalog')
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
        logging.info('Inserting CSV files')
        self.insert()


class Ps1StrmArgSubParser(ArgSubParser):
    command = 'ps1-strm'
    putter_cls = Ps1StrmPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs "insert" actions')
