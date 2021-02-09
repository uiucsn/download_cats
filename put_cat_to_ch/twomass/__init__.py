import logging
from typing import List, Tuple

import bs4

from download_cats.utils import url_text_content
from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.twomass import sql, sh
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.shell_runner import ShellRunner


def printf_to_ch(fmt):
    assert fmt.startswith('%'), f'format {fmt} is unknown'
    fmt = fmt[1:]
    if fmt.endswith('f'):
        size, *_ = fmt.split('.')
        size = int(size)
        if size <= 7:
            return 'Float32'
        return 'Float64'
    if fmt.endswith('d'):
        size = int(fmt[:-1])
        if size <= 2:
            return 'Int8'
        if size <= 4:
            return 'Int16'
        if size <= 9:
            return 'Int32'
        return 'Int64'
    if fmt.endswith('s'):
        size = int(fmt[:-1])
        return f'FixedString({size})'
    raise ValueError(f'Format {fmt} is unknown')


def table_row_to_ch_type(cells: List[bs4.element.Tag]) -> Tuple[str, str]:
    name, fmt, _units, nulls, _description, *_ = cells
    name, *_ = name.text.strip().split('/')
    ch_type = printf_to_ch(fmt.text.strip())
    nulls = nulls.text.strip()
    if nulls == 'yes':
        ch_type = f'Nullable({ch_type})'
    return name, ch_type


def get_psc_columns():
    url = 'https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html'
    html = url_text_content(url)
    bs = bs4.BeautifulSoup(html, 'lxml')
    columns = {}
    for tr in bs.find_all('tr'):
        cells = tr.find_all('td')
        if len(cells) < 5:
            continue
        name, ch_type = table_row_to_ch_type(cells)
        columns[name] = ch_type
    return columns


class TwoMASSPutter(CHPutter):
    db = 'twomass'
    psc_table = 'psc'

    def __init__(self, dir, user, host, clickhouse_settings, on_exists, **_kwargs):
        self.dir = dir
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

    def ch_columns_str(self):
        columns = get_psc_columns()
        s = ',\n    '.join(f'{name} {ch_type}' for name, ch_type in columns.items())
        return s

    def create_psc_table(self, on_exists: str):
        exists_ok = self.process_on_exists(on_exists, self.db, self.psc_table)
        self.exe_query(
            'create_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.psc_table,
            columns=self.ch_columns_str(),
        )

    def insert_into_pcs_table(self):
        self.shell_runner('insert_into_pcs.sh', self.dir, f'{self.db}.{self.psc_table}', self.host)

    default_actions = ('create', 'insert',)

    def action_create(self):
        self.create_psc_table(self.on_exists)

    def action_insert(self):
        self.insert_into_pcs_table()


class DustMapsArgSubParser(ArgSubParser):
    command = '2mass'
    putter_cls = TwoMASSPutter
