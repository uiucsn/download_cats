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


class ZtfPutter:
    db = 'ztf'
    obs_table = 'dr3_obs'
    meta_table = 'dr3_meta'
    circle_table_parts = 16

    def __init__(self, *, dir, user, host, jobs, on_exists, radius, **_kwargs):
        self.data_dir = dir
        self.user = user
        self.host = host
        self.processes = jobs
        self.on_exists = on_exists
        self.radius_arcsec = radius
        self.client = Client(
            host=self.host,
            database=self.db,
            user=self.user,
            settings={
                'max_bytes_before_external_group_by': 1 << 34,
                # 'join_algorithm': 'auto',
                # 'default_max_bytes_in_join': 1 << 35,
                'aggregation_memory_efficient_merge_threads': 1,
            }
        )

    @property
    def radius_table_suffix(self):
        return f'{self.radius_arcsec:.2f}'.replace('.', '').rstrip('0')

    @property
    def circle_match_table(self):
        return f'dr3_circle_match_{self.radius_table_suffix}'

    @property
    def xmatch_table(self):
        return f'dr3_xmatch_{self.radius_table_suffix}'

    @property
    def lc_table(self):
        return f'dr3_lc_{self.radius_table_suffix}'

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
        path_template = os.path.join(self.data_dir, 'field*.tar.gz')
        file_paths = sorted(glob(path_template))
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_data_into_obs_table_worker, file_paths, chunksize=1)

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

    def get_min_max_quantiles(self, column: str, levels: Iterable[float], *, db: Optional[str] = None, table: str):
        if db is None:
            db = self.db
        levels_str = ', '.join(map(str, levels))
        query = f'''
        SELECT
            min({column}),
            max({column}),
            quantiles({levels_str})({column})
        FROM {db}.{table}
        '''
        result = self.client.execute(query)
        return result[0]

    def _insert_data_into_circle_table(self, begin_oid, end_oid):
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

    def insert_data_into_circle_table(self, parts=1):
        if parts < 1:
            msg = f'parts should be positive, not {parts}'
            logging.warning(msg)
            raise ValueError(msg)
        if parts == 1:
            min_oid, max_oid = self.get_min_max(column='oid', table=self.meta_table)
            grid = np.array([min_oid, max_oid + 1])
        else:
            levels = np.linspace(0, 1, parts, endpoint=False)[1:]
            min_oid, max_oid, q = self.get_min_max_quantiles(column='oid', levels=levels, table=self.meta_table)
            grid = np.r_[min_oid, q, max_oid + 1]
        assert np.all(np.diff(grid) > 0), 'grid must be monotonically increasing'
        for begin_oid, end_oid in zip(grid[:-1], grid[1:]):
            self._insert_data_into_circle_table(begin_oid, end_oid)

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

    def create_lc_table(self, on_exists: str = 'fail'):
        exists_ok = self.process_on_exists(on_exists, self.lc_table)
        self.exe_query(
            'create_lc_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            lc_db=self.db,
            lc_table=self.lc_table,
        )

    def insert_data_into_lc_table(self, on_exists: str = 'fail'):
        self.exe_query(
            'insert_into_lc_table.sql',
            lc_db=self.db,
            lc_table=self.lc_table,
            obs_db=self.db,
            obs_table=self.obs_table,
            xmatch_db=self.db,
            xmatch_table=self.xmatch_table,
        )

    def __call__(self, actions):
        if 'insert-obs' in actions:
            self.create_db()
            self.create_obs_table(on_exists=self.on_exists)
            self.insert_data_into_obs_table()
        if 'insert-meta' in actions:
            self.create_obs_meta_table(on_exists=self.on_exists)
            self.insert_data_into_circle_table(parts=self.circle_table_parts)
            self.insert_data_into_obs_meta_table()
        if 'xmatch' in actions:
            self.create_circle_table(on_exists=self.on_exists)
            self.insert_data_into_circle_table(parts=self.circle_table_parts)
            self.create_xmatch_table(on_exists=self.on_exists)
            self.insert_into_xmatch_table()
        if 'insert-lc' in actions:
            self.create_lc_table(on_exists=self.on_exists)
            self.insert_data_into_lc_table()
