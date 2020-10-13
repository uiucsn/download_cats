import importlib.resources
import logging
import os
from glob import glob
from multiprocessing.pool import ThreadPool
from subprocess import check_call
from typing import Dict

from clickhouse_driver import Client

from put_cat_to_ch.ztf import sh, sql


class ZtfPutter:
    db = 'ztf'
    obs_table = 'dr3_obs'
    meta_table = 'dr3_meta'

    def __init__(self, *, dir, user, host, jobs, on_exists, **_kwargs):
        self.data_dir = dir
        self.user = user
        self.host = host
        self.processes = jobs
        self.on_exists = on_exists
        url = f'clickhouse://{self.user}@{self.host}/'
        self.client = Client.from_url(url)

    @staticmethod
    def get_query(filename: str, **format_kwargs: str) -> str:
        template = importlib.resources.read_text(sql, filename)
        query = template.format(**format_kwargs)
        return query

    def exe_query(self, filename: str, **format_kwargs: str):
        query = self.get_query(filename, **format_kwargs)
        self.client.execute(query)

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
        result = self.client.execute(f'EXISTS TABLE {self.db}.{table_name}')
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

    def create_obs_table(self, on_exists: str = 'fail'):
        """Create observations table

        Parameters
        ----------
        on_exists : str
            If table already exists than:
            - 'fail' : throw a DB::Exception
            - 'keep' : do nothing
            - 'drop' : drop and create the table again
        """
        logging.info(f'Creating table {self.db}.{self.obs_table}')
        if on_exists == 'fail':
            exists_ok = False
        elif on_exists == 'keep':
            exists_ok = True
        elif on_exists == 'drop':
            exists_ok = False
            if self.table_exists(self.obs_table):
                self.drop_obs_table(not_exists_ok=False)
        else:
            msg = f'on_exists must be either "fail" or "keep" or "drop", not {on_exists}'
            logging.warning(msg)
            raise ValueError(msg)
        self.exe_query(
            'create_obs_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.obs_table,
        )

    def drop_obs_table(self, not_exists_ok: bool = False):
        logging.info(f'Dropping table {self.db}.{self.obs_table}')
        self.exe_query(
            'drop_obs_table.sql',
            if_exists=self.if_exists(not_exists_ok),
            db=self.db,
            table=self.obs_table,
        )

    def insert_data_into_obs_table_worker(self, filepath: str):
        self.run_script('insert_field_file.sh', filepath, f'{self.db}.{self.obs_table}', self.host)

    def insert_data_into_obs_table(self):
        path_template = os.path.join(self.data_dir, 'field*.tar.gz')
        file_paths = sorted(glob(path_template))
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_data_into_obs_table_worker, file_paths)

    def __call__(self):
        self.create_db()
        self.create_obs_table(on_exists=self.on_exists)
        self.insert_data_into_obs_table()
