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
        self.client.execute(query)

    def drop_table(self, db: str, table: str, not_exists_ok: bool = False):
        logging.info(f'Drop table {db}.{table}')
        if_exists = self.if_exists(not_exists_ok)
        query = f'DROP TABLE {if_exists} {db}.{table}'
        self.client.execute(query)

    def exists_table(self, db: str, table: str) -> bool:
        logging.info(f'Checking if table {db}.{table} exists')

        query = f'EXISTS TABLE {db}.{table}'
        result = self.client.execute(query)

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
        return self.client.execute(query)

    def execute(self, query: str) -> List[Tuple]:
        logging.info(f'Executing {query}')
        return self.client.execute(query)
