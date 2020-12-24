import logging
from importlib.resources import read_text
from types import ModuleType
from typing import List, Tuple

from clickhouse_driver import Client


class CHClient:
    def __init__(self, module: ModuleType, **kwargs):
        self.module = module
        self.client = Client(**kwargs)

    # TODO: if python 3.7 support can be dropped, replace signature to (x=True, /)
    @staticmethod
    def if_exists(x: bool = True) -> str:
        if x:
            return 'IF EXISTS'
        return ''

    # TODO: if python 3.7 support can be dropped, replace signature to (x=True, /)
    @staticmethod
    def if_not_exists(x: bool = True) -> str:
        if x:
            return 'IF NOT EXISTS'
        return ''

    def create_db(self, db: str):
        logging.info(f'Creating database {db}')
        query = f'CREATE DATABASE IF NOT EXISTS {db}'
        self.execute(query)

    def drop_table(self, db: str, table: str, not_exists_ok: bool = False):
        logging.info(f'Drop table {db}.{table}')
        if_exists = self.if_exists(not_exists_ok)
        query = f'DROP TABLE {if_exists} {db}.{table}'
        self.execute(query)

    def exists_table(self, db: str, table: str) -> bool:
        logging.info(f'Checking if table {db}.{table} exists')

        query = f'EXISTS TABLE {db}.{table}'
        result = self.execute(query)

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

    def _get_query(self, filename: str, **format_kwargs: str) -> str:
        template = read_text(self.module, filename)
        query = template.format(**format_kwargs)
        return query

    def exe_query(self, filename: str, **format_kwargs: str) -> List[Tuple]:
        logging.info(f'Going to execute query from file {filename}')
        query = self._get_query(filename, **format_kwargs)
        return self.execute(query)

    def execute(self, query: str) -> List[Tuple]:
        logging.info(f'Executing {query}')
        return self.client.execute(query)

    def process_on_exists(self, on_exists: str, db: str, table_name: str) -> bool:
        """Process "on_exists" politics and return exists_ok

        Parameters
        ----------
        on_exists : str
            If table already exists than:
            - 'fail' : throw a DB::Exception
            - 'keep' : do nothing
            - 'drop' : drop and create the table again
            Actually this method doesn't tries to create new table, so 'fail'
            will throw an exception during actual table creation only
        db : str
            Database containing the table
        table_name : str
            Table to process
        """
        logging.info(f'Creating table {db}.{table_name}')

        if on_exists == 'fail':
            return False
        elif on_exists == 'keep':
            return True
        elif on_exists == 'drop':
            if self.exists_table(db, table_name):
                self.drop_table(db, table_name, not_exists_ok=False)
            return False

        msg = f'on_exists must be one of "fail" or "keep" or "drop", not {on_exists}'
        logging.warning(msg)
        raise ValueError(msg)
