import argparse
from multiprocessing.pool import ThreadPool
from pathlib import Path

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.ps1 import sql, sh
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner


class Ps1Putter(CHPutter):
    db = 'ps1'
    table = 'otmo'

    def __init__(self, dir, user, host, jobs, clickhouse_settings, on_exists, **_kwargs):
        self.dir = dir
        self.on_exists = on_exists
        self.user = user
        self.host = host
        self.processes = jobs
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

    def create_table(self, on_exists: str):
        exists_ok = self.process_on_exists(on_exists, self.db, self.table)
        self.exe_query(
            f'create_{self.table}_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.table,
        )

    def insert_one_file_into_table(self, file: str):
        self.shell_runner('insert.sh', file, f'{self.db}.{self.table}', self.host)

    def insert_into_table(self):
        files = sorted(map(str, Path(self.dir).glob('*.csv')))
        if len(files) == 0:
            raise RuntimeError(f'No files found in {self.dir}')
        with ThreadPool(self.processes) as pool:
            pool.map(self.insert_one_file_into_table, files)

    default_actions = ('create', 'insert',)

    def action_create(self):
        self.create_table(self.on_exists)

    def action_insert(self):
        self.insert_into_table()


class Ps1ArgSubParser(ArgSubParser):
    command = 'ps1'
    putter_cls = Ps1Putter

    def __init__(self, cli_args: argparse.Namespace):
        super().__init__(cli_args)

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-j', '--jobs', default=1, type=int, help='number of parallel field insert jobs')
