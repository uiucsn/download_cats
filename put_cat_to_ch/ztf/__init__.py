import importlib.resources
import logging
import os
from glob import glob
from multiprocessing.pool import ThreadPool
from subprocess import check_call
from typing import List, Tuple, Iterable, Optional

import numpy as np
from clickhouse_driver import Client

from put_cat_to_ch.ztf import sh, sql


CURRENT_ZTF_DR = 4


class ZtfPutter:
    db = 'ztf'

    _default_settings = {
        'max_bytes_before_external_group_by': 1 << 34,
        # 'join_algorithm': 'auto',
        # 'default_max_bytes_in_join': 1 << 35,
        'aggregation_memory_efficient_merge_threads': 1,
        # 'max_threads': 1,
    }

    def __init__(self, *, dir, csv_dir, dr, user, host, jobs, on_exists, radius, circle_match_insert_parts,
                 source_obs_insert_parts, clickhouse_settings, **_kwargs):
        self.data_dir = dir
        self.csv_dir = csv_dir
        if self.csv_dir is None:
            self.csv_dir = os.path.join(self.data_dir, 'csv')
        self.dr = dr
        self.user = user
        self.host = host
        self.processes = jobs
        self.on_exists = on_exists
        self.radius_arcsec = radius
        self.circle_table_parts = circle_match_insert_parts
        self.source_obs_table_parts = source_obs_insert_parts
        self.settings = self._default_settings
        self.settings.update(clickhouse_settings)
        self.client = Client(
            host=self.host,
            database=self.db,
            user=self.user,
            settings=self.settings,
            connect_timeout=86400,
            send_receive_timeout=86400,
            sync_request_timeout=86400,
        )

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
    def get_query(filename: str, **format_kwargs: str) -> str:
        template = importlib.resources.read_text(sql, filename)
        query = template.format(**format_kwargs)
        return query

    def exe_query(self, filename: str, **format_kwargs: str) -> List[Tuple]:
        query = self.get_query(filename, **format_kwargs)
        return self.client.execute(query)

    def create_db(self):
        self.exe_query('create_db.sql', db=self.db)

    # TODO: if python 3.7 support can be dropped, replace signature to (x=True, /)
    @staticmethod
    def if_not_exists(x: bool = True) -> str:
        if x:
            return 'IF NOT EXISTS'
        return ''

    # TODO: if python 3.7 support can be dropped, replace signature to (x=True, /)
    @staticmethod
    def if_exists(x: bool = True) -> str:
        if x:
            return 'IF EXISTS'
        return ''

    @staticmethod
    def run_script(filename: str, *args: str) -> int:
        script = importlib.resources.read_text(sh, filename)
        args = ('sh', '-c', script, filename) + args
        logging.info(f'Executing {" ".join(args)}')
        return check_call(args)

    def table_exists(self, table_name: str) -> bool:
        logging.info(f'Checking if table {self.db}.{table_name} exists')
        result = self.exe_query('exists_table.sql', db=self.db, table=table_name)
        try:
            value = result[0][0]
        except KeyError:
            msg = f'Unexpected result from ClickHouse: {result}'
            logging.error(msg)
            raise RuntimeError(msg)
        if value == 1:
            return True
        elif value == 0:
            return False
        msg = f'Unexpected result from ClickHouse: {result}'
        logging.error(msg)
        raise RuntimeError(msg)

    def process_on_exists(self, on_exists: str, table_name: str) -> bool:
        """Process "on_exists" politics and return exists_ok

        Parameters
        ----------
        on_exists : str
            If table already exists than:
            - 'fail' : throw a DB::Exception
            - 'keep' : do nothing
            - 'drop' : drop and create the table again
        table_name : str
            Table to process
        """
        logging.info(f'Creating table {self.db}.{table_name}')
        if on_exists == 'fail':
            return False
        elif on_exists == 'keep':
            return True
        elif on_exists == 'drop':
            if self.table_exists(table_name):
                self.drop_table(table_name, not_exists_ok=False)
            return False
        msg = f'on_exists must be one of "fail" or "keep" or "drop", not {on_exists}'
        logging.warning(msg)
        raise ValueError(msg)

    def drop_table(self, table_name: str, not_exists_ok: bool = False):
        logging.info(f'Dropping table {self.db}.{table_name}')
        self.exe_query(
            'drop_table.sql',
            if_exists=self.if_exists(not_exists_ok),
            db=self.db,
            table=table_name,
        )

    def tar_gz_files(self) -> List[str]:
        path_template = os.path.join(self.data_dir, 'field*.tar.gz')
        file_paths = sorted(glob(path_template))
        return file_paths

    def tar_gz_to_csv(self, path):
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
        self.run_script('generate_csv.sh', input_path, output_path)

    def generate_csv(self):
        os.makedirs(self.csv_dir, exist_ok=True)
        with ThreadPool(self.processes) as pool:
            pool.starmap(self.generate_csv_worker, zip(self.tar_gz_files(), self.csv_files()), chunksize=1)

    def create_obs_table(self, on_exists: str = 'fail'):
        """Create observations table"""
        exists_ok = self.process_on_exists(on_exists, self.obs_table)
        self.exe_query(
            'create_obs_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.obs_table,
        )

    def create_obs_meta_table(self, on_exists: str = 'fail'):
        """Create table containing ZTF object information"""
        exists_ok = self.process_on_exists(on_exists, self.meta_table)
        self.exe_query(
            'create_obs_meta_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.meta_table,
        )

    def insert_data_into_obs_table_worker(self, filepath: str):
        self.run_script('insert_field_file.sh', filepath, f'{self.db}.{self.obs_table}', self.host)

    def insert_data_into_obs_table(self):
        file_paths = self.tar_gz_files()
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_data_into_obs_table_worker, file_paths, chunksize=1)

    def insert_csv_into_obs_table_worker(self, filepath: str):
        self.run_script('insert_csv.sh', filepath, f'{self.db}.{self.obs_table}', self.host)

    def insert_csv_into_obs_table(self):
        csv_paths = self.csv_files()
        for path in csv_paths:
            self.insert_csv_into_obs_table_worker(path)

    def remove_csv(self):
        paths = self.csv_files()
        for path in paths:
            os.remove(path)
        try:
            os.rmdir(self.csv_dir)
        except OSError:  # dir is not empty
            pass

    def insert_data_into_obs_meta_table(self):
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
        exists_ok = self.process_on_exists(on_exists, self.circle_match_table)
        self.exe_query(
            'create_circle_match_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.circle_match_table,
        )

    def get_min_max(self, column: str, *, db: Optional[str] = None, table: str):
        if db is None:
            db = self.db
        query = f'SELECT min({column}), max({column}) FROM {db}.{table}'
        result = self.client.execute(query)
        return result[0]

    def get_quantiles(self, column: str, levels: Iterable[float], *, db: Optional[str] = None, table: str):
        if db is None:
            db = self.db
        levels_str = ', '.join(map(str, levels))
        query = f'''
        SELECT
            quantiles({levels_str})({column})
        FROM {db}.{table}
        '''
        result = self.client.execute(query)
        return result[0][0]

    # Improve typing when numpy 1.20 will arrive
    def construct_quantile_grid(self, parts: int, **kwargs) -> np.ndarray:
        """Create grid to split some column data range to

        Parameters
        ----------
        parts : int
            Positive number. If zero, then [-INF, +INF] is returned
        **kwargs
            All arguments of `get_min_max_quantiles` but `levels`
        """
        if parts < 1:
            msg = f'parts should be positive, not {parts}'
            logging.warning(msg)
            raise ValueError(msg)
        if parts == 1:
            return np.array([-np.inf, np.inf])
        levels = np.linspace(0, 1, parts, endpoint=False)[1:]
        q = self.get_quantiles(levels=levels, **kwargs)
        assert np.all(np.diff(q) > 0), 'quantiles must be monotonically increasing'
        grid = np.r_[-np.inf, q, np.inf]
        return grid

    def insert_data_into_circle_table(self, parts: int = 1):
        grid = self.construct_quantile_grid(parts, column='oid', table=self.meta_table)
        for begin_oid, end_oid in zip(grid[:-1], grid[1:]):
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
        exists_ok = self.process_on_exists(on_exists, self.xmatch_table)
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
        exists_ok = self.process_on_exists(on_exists, self.source_obs_table)
        self.exe_query(
            'create_source_obs_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.source_obs_table,
        )

    def insert_into_source_obs_table(self, parts: int = 1):
        grid = self.construct_quantile_grid(parts, column='oid1', table=self.xmatch_table)
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
        exists_ok = self.process_on_exists(on_exists, self.source_meta_table)
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

    def __call__(self, actions):
        if 'gen-csv' in actions:
            self.generate_csv()
        if 'obs' in actions:
            self.create_db()
            self.create_obs_table(on_exists=self.on_exists)
            self.insert_csv_into_obs_table()
        if 'rm-csv' in actions:
            self.remove_csv()
        if 'meta' in actions:
            self.create_obs_meta_table(on_exists=self.on_exists)
            self.insert_data_into_obs_meta_table()
        if 'circle' in actions:
            self.create_circle_table(on_exists=self.on_exists)
            self.insert_data_into_circle_table(parts=self.circle_table_parts)
        if 'xmatch' in actions:
            self.create_xmatch_table(on_exists=self.on_exists)
            self.insert_into_xmatch_table()
        if 'source-obs' in actions:
            self.create_source_obs_table(on_exists=self.on_exists)
            self.insert_into_source_obs_table(parts=self.source_obs_table_parts)
        if 'source-meta' in actions:
            self.create_source_meta_table(on_exists=self.on_exists)
            self.insert_into_source_meta_table()
