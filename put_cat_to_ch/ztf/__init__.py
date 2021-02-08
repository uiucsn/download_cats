import argparse
import logging
import os
import re
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import List, Tuple, Iterable, Optional, Union

import numpy as np

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.utils import remove_files_and_directory
from put_cat_to_ch.ztf import sh, sql


__all__ = ('ZtfPutter', 'ZtfArgSubParser',)


CURRENT_ZTF_DR = 4


class ZtfPutter(CHPutter):
    db = 'ztf'

    _default_settings = {
        'max_bytes_before_external_group_by': 1 << 34,
        # 'join_algorithm': 'auto',
        # 'default_max_bytes_in_join': 1 << 35,
        'aggregation_memory_efficient_merge_threads': 1,
        # 'max_threads': 1,
        'persistent': 0,  # turn off persistency of Set and Join tables
    }

    def __init__(self, *, dir, tmp_dir, dr, user, host, jobs, on_exists, start_field, end_field, radius,
                 circle_match_insert_parts, circle_match_insert_interval, source_obs_insert_parts, clickhouse_settings,
                 **_kwargs):
        self.data_dir = dir
        self.csv_dir = tmp_dir or self.data_dir
        self.dr = dr
        self.user = user
        self.host = host
        self.processes = jobs
        self.on_exists = on_exists
        self.start_csv_field = start_field
        self.end_csv_field = end_field
        self.radius_arcsec = radius
        self.circle_table_parts = circle_match_insert_parts
        self.circle_table_interval = circle_match_insert_interval
        self.source_obs_table_parts = source_obs_insert_parts
        self.settings = self._default_settings
        self.settings.update(clickhouse_settings)
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

    @property
    def radius_table_suffix(self):
        return f'{self.radius_arcsec:.2f}'.replace('.', '').rstrip('0')

    @property
    def obs_table(self):
        return f'dr{self.dr:d}_obs'

    @property
    def meta_table(self):
        return f'dr{self.dr:d}_meta'

    @property
    def circle_match_table(self):
        return f'dr{self.dr:d}_circle_match_{self.radius_table_suffix}'

    @property
    def xmatch_table(self):
        return f'dr{self.dr:d}_xmatch_{self.radius_table_suffix}'

    @property
    def source_obs_table(self):
        return f'dr{self.dr:d}_source_obs_{self.radius_table_suffix}'

    @property
    def source_meta_table(self):
        return f'dr{self.dr:d}_source_meta_{self.radius_table_suffix}'

    @staticmethod
    def extract_field_number(path: str) -> int:
        basename = os.path.basename(path)
        match = re.search(r'^field(\d+)', basename)
        field_no = int(match.group(1))
        return field_no

    def tar_gz_files(self) -> List[str]:
        path_template = os.path.join(self.data_dir, 'field*.tar.gz')
        file_paths = sorted(glob(path_template))
        return file_paths

    def tar_gz_to_csv(self, path: str) -> str:
        assert path.endswith('.tar.gz')
        basename = os.path.basename(path)
        field_name = os.path.splitext(os.path.splitext(basename)[0])[0]
        csv_name = f'{field_name}.csv'
        csv_path = os.path.join(self.csv_dir, csv_name)
        return csv_path

    def csv_files(self) -> List[str]:
        tar_gz_paths = self.tar_gz_files()
        csv_paths = [self.tar_gz_to_csv(path) for path in tar_gz_paths]
        return csv_paths

    def generate_csv_worker(self, input_path, output_path):
        logging.info(f'Generate CSV from .tar.gz: {input_path} -> {output_path}')
        self.shell_runner('generate_csv.sh', input_path, output_path)

    def generate_csv(self):
        logging.info('Generationg CSV field files')
        os.makedirs(self.csv_dir, exist_ok=True)
        with ThreadPool(self.processes) as pool:
            pool.starmap(self.generate_csv_worker, zip(self.tar_gz_files(), self.csv_files()), chunksize=1)

    def create_obs_table(self, on_exists: str = 'fail'):
        """Create observations table"""
        exists_ok = self.process_on_exists(on_exists, self.db, self.obs_table)
        self.exe_query(
            'create_obs_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.obs_table,
        )

    def create_obs_meta_table(self, on_exists: str = 'fail'):
        """Create table containing ZTF object information"""
        exists_ok = self.process_on_exists(on_exists, self.db, self.meta_table)
        self.exe_query(
            'create_obs_meta_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.meta_table,
        )

    def insert_tar_gz_into_obs_table_worker(self, filepath: str):
        logging.info(f'Inserting {filepath} into {self.obs_table}')
        self.shell_runner('insert_field_file.sh', filepath, f'{self.db}.{self.obs_table}', self.host)

    def insert_tar_gz_into_obs_table(self):
        logging.info(f'Inserting .tar.gz field files into {self.obs_table}')
        tar_gz_paths = self.tar_gz_files()
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_tar_gz_into_obs_table_worker, tar_gz_paths, chunksize=1)

    def insert_csv_into_obs_table_worker(self, filepath: str):
        logging.info(f'Inserting {filepath} info {self.obs_table}')
        self.shell_runner('insert_csv.sh', filepath, f'{self.db}.{self.obs_table}', self.host)

    def insert_csv_into_obs_table(self, start: Optional[int] = None, end: Optional[int] = None):
        logging.info(f'Inserting CSV field files into {self.obs_table}')
        csv_paths = self.csv_files()
        if (start is not None) or (end is not None):
            field_numbers = [self.extract_field_number(path) for path in csv_paths]
            if start is None:
                start = min(field_numbers)
            if end is None:
                end = max(field_numbers)
            csv_paths = [path for path, field_no in zip(csv_paths, field_numbers) if start <= field_no <= end]
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_csv_into_obs_table_worker, csv_paths, chunksize=1)

    def remove_csv(self):
        logging.info(f'Removing CSV field files from {self.csv_dir}')
        remove_files_and_directory(self.csv_dir, self.csv_files())

    def insert_data_into_obs_meta_table(self):
        logging.info(f'Inserting data into {self.meta_table}')
        self.exe_query(
            'insert_into_obs_meta_table.sql',
            meta_db=self.db,
            meta_table=self.meta_table,
            obs_db=self.db,
            obs_table=self.obs_table,
        )

    def create_circle_table(self, on_exists: str = 'fail'):
        """Create self-match table for circle search

        It will be used to create xmatch table
        """
        exists_ok = self.process_on_exists(on_exists, self.db, self.circle_match_table)
        self.exe_query(
            'create_circle_match_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.circle_match_table,
        )

    # TODO: replace bool with Literal[False] when python 3.7 support could be droped
    def get_quantiles(self, column: str, levels: Iterable[float], *, db: Optional[str] = None, table: str,
                      column_determinator: Union[None, bool, str] = None):
        """Get approximate quantiles of column destribution
        
        column : str
            Column name
        levels : iterable of floats
            Quantile levels
        db : str
            Databse name
        table : str
            Table name
        column_determinator : None, False or str
            Column used to generate random numbers for reservoir sampling
            algorithm used by CH to find approximate quantile values.
            This column must have positive elements, preferably unique.
            `None` means use `column`, `False` means use non-determenistic
            quantile algorithm.
        """
        if db is None:
            db = self.db
        levels_str = ', '.join(map(str, levels))
        if column_determinator is False:
            function = f'quantiles({levels_str})({column})'
        else:
            if column_determinator is None:
                column_determinator = column
            function = f'quantilesDeterministic({levels_str})({column}, {column_determinator})'
        query = f'''
        SELECT
            {function}
        FROM {db}.{table}
        '''
        result = self.execute(query)
        return result[0][0]

    # Improve typing when numpy 1.20 will arrive
    def construct_quantile_grid(self, parts: int, dtype: type = np.float64, **kwargs) -> List:
        """Create grid to split some column data range to

        Parameters
        ----------
        parts : int
            Positive number. If zero, then [-INF, +INF] is returned
        dtype : numpy dtype
            Convert to
        **kwargs
            All arguments of `get_min_max_quantiles` but `levels`
        """
        if parts < 1:
            msg = f'parts should be positive, not {parts}'
            logging.warning(msg)
            raise ValueError(msg)
        if parts == 1:
            return [-np.inf, np.inf]
        levels = np.linspace(0, 1, parts, endpoint=False)[1:]
        q = self.get_quantiles(levels=levels, **kwargs)
        q = np.asarray(q, dtype=dtype)
        assert np.all(q[1:] > q[:-1]), f'quantiles must be monotonically increasing: {q}'
        grid = [-np.inf, *q, np.inf]
        return grid

    # TODO: replace str with Literal['all'] when python 3.7 could be droped
    def insert_data_into_circle_table(self, parts: int = 1, interval: Union[int, str] = 'all'):
        """Insert data into cirle_match table
        
        Arguments
        ---------
        parts : int
            Number of parts to split initial table to perform insertion
        interval : int or 'all'
            Which part to insert, either 'all' or int from `0` to `parts - 1`
        """
        grid = self.construct_quantile_grid(parts, dtype=np.uint64, column='oid', table=self.meta_table)
        if interval == 'all':
            intervals = zip(grid[:-1], grid[1:])
        else:
            intervals = [(grid[interval], grid[interval + 1])]
        for begin_oid, end_oid in intervals:
            self.exe_query(
                'insert_into_circle_match_table.sql',
                circle_db=self.db,
                circle_table=self.circle_match_table,
                radius_arcsec=self.radius_arcsec,
                meta_db=self.db,
                meta_table=self.meta_table,
                begin_oid=begin_oid,
                end_oid=end_oid,
            )

    def create_xmatch_table(self, on_exists: str = 'fail'):
        """Create self cross-match table"""
        exists_ok = self.process_on_exists(on_exists, self.db, self.xmatch_table)
        self.exe_query(
            'create_xmatch_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.xmatch_table,
        )

    def insert_into_xmatch_table(self):
        self.exe_query(
            'insert_into_xmatch_table.sql',
            xmatch_db=self.db,
            xmatch_table=self.xmatch_table,
            circle_db=self.db,
            circle_table=self.circle_match_table,
        )

    def create_source_obs_table(self, on_exists: str = 'fail'):
        """Create self cross-match table"""
        exists_ok = self.process_on_exists(on_exists, self.db, self.source_obs_table)
        self.exe_query(
            'create_source_obs_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.source_obs_table,
        )

    def insert_into_source_obs_table(self, parts: int = 1):
        grid = self.construct_quantile_grid(parts, dtype=np.uint64, column='oid1', table=self.xmatch_table)
        for begin_oid, end_oid in zip(grid[:-1], grid[1:]):
            self.exe_query(
                'insert_into_source_obs_table.sql',
                source_obs_db=self.db,
                source_obs_table=self.source_obs_table,
                obs_db=self.db,
                obs_table=self.obs_table,
                xmatch_db=self.db,
                xmatch_table=self.xmatch_table,
                begin_oid=begin_oid,
                end_oid=end_oid,
            )

    def create_source_meta_table(self, on_exists: str = 'fail'):
        exists_ok = self.process_on_exists(on_exists, self.db, self.source_meta_table)
        self.exe_query(
            'create_source_meta_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.source_meta_table,
        )

    def insert_into_source_meta_table(self):
        self.exe_query(
            'insert_into_source_meta_table.sql',
            source_meta_db=self.db,
            source_meta_table=self.source_meta_table,
            source_obs_db=self.db,
            source_obs_table=self.source_obs_table,
        )

    default_actions = ('gen_csv', 'csv_obs', 'rm_csv', 'meta', 'circle', 'xmatch', 'source_obs', 'source_meta',)

    def action_gen_csv(self):
        self.generate_csv()

    def action_csv_obs(self):
            self.create_db(self.db)
            self.create_obs_table(on_exists=self.on_exists)
            self.insert_csv_into_obs_table(start=self.start_csv_field, end=self.end_csv_field)

    def action_rm_csv(self):
        self.remove_csv()

    def action_tar_gz_obs(self):
        self.create_db(self.db)
        self.create_obs_table(on_exists=self.on_exists)
        self.insert_tar_gz_into_obs_table()

    def action_meta(self):
        self.create_obs_meta_table(on_exists=self.on_exists)
        self.insert_data_into_obs_meta_table()

    def action_circle(self):
        self.create_circle_table(on_exists=self.on_exists)
        self.insert_data_into_circle_table(parts=self.circle_table_parts, interval=self.circle_table_interval)

    def action_xmatch(self):
        self.create_xmatch_table(on_exists=self.on_exists)
        self.insert_into_xmatch_table()

    def action_source_obs(self):
        self.create_source_obs_table(on_exists=self.on_exists)
        self.insert_into_source_obs_table(parts=self.source_obs_table_parts)

    def action_source_meta(self):
        self.create_source_meta_table(on_exists=self.on_exists)
        self.insert_into_source_meta_table()


class ZtfArgSubParser(ArgSubParser):
    command = 'ztf'
    putter_cls = ZtfPutter

    def __init__(self, cli_args: argparse.Namespace):
        super().__init__(cli_args)

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('--dr', default=CURRENT_ZTF_DR, type=int, help='ZTF DR number')
        parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel field insert jobs')
        parser.add_argument('--start-field', default=None, type=int, help='specify the first field file to insert')
        parser.add_argument('--end-field', default=None, type=int,
                            help='specify the last field file to insert (it is included)')
        parser.add_argument('-r', '--radius', default=0.2, type=float, help='cross-match radius, arcsec')
        parser.add_argument('--circle-match-insert-parts', default=1, type=int,
                            help='specifies the number of parts to split meta table to perform insert into '
                                 'circle-match table, execution time proportional to number of parts, '
                                 'but RAM usage is inversly proportional to it')
        parser.add_argument('--circle-match-insert-interval', default='all',
                            type=lambda s: s if s == 'all' else int(s),
                            help='which part of meta table insert to circle table now, default is '
                                 'inserting all parts sequentially')
        parser.add_argument('--source-obs-insert-parts', default=1, type=int,
                            help='same as --circle-match-insert-parts but for source-obs table')
