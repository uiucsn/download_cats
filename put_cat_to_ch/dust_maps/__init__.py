import argparse
import logging
from itertools import islice
from typing import Generator

import dustmaps.sfd
import h3
import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.dust_maps import sql
from put_cat_to_ch.putter import CHPutter


def gen_h3index(res: int) -> Generator[str, None, None]:
    if res < 0:
        return
    # All zero-resolution tiles
    indices = h3.k_ring(h3.geo_to_h3(0.0, 0.0, 0), 10)
    if res == 0:
        yield from indices
        return
    for index in indices:
        yield from _gen_h3_children(index, cur_res=0, max_res=res)


def _gen_h3_children(index: str, *, cur_res: int, max_res: int) -> Generator[str, None, None]:
    children = h3.h3_to_children(index)
    cur_res += 1
    if cur_res == max_res:
        yield from children
        return
    for child in children:
        yield from _gen_h3_children(child, cur_res=cur_res, max_res=max_res)


class SingleDustMapPutter:
    def __init__(self, ch_client, tmp_dir, chunksize=1 << 20):
        self.ch_client = ch_client
        self.db = ch_client.db
        self.tmp_dir = tmp_dir
        self.chunksize = chunksize


class SFDPutter(SingleDustMapPutter):
    table = 'sfd'
    h3_res = 7

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        dustmaps.sfd.config['data_dir'] = self.tmp_dir
        dustmaps.sfd.fetch()
        self.dustmap_query = dustmaps.sfd.SFDQuery()

    def create_table(self, on_exists: str):
        exists_ok = self.ch_client.process_on_exists(on_exists, self.db, self.table)
        self.ch_client.exe_query(
            'create_SFD_table.sql',
            if_not_exists=self.ch_client.if_not_exists(exists_ok),
            db=self.db,
            table=self.table,
        )

    def gen_data(self):
        logging.info('Collecting SDF dust map data')

        gen = gen_h3index(self.h3_res)
        while True:
            chunk_gen = islice(gen, self.chunksize)
            df = pd.DataFrame(
                [(np.uint64(int(i, base=16)), *h3.h3_to_geo(i)) for i in chunk_gen],
                columns=(f'h3index{self.h3_res}', 'dec', 'ra',)  # h3_to_geo returns (lat, lon)
            )
            if df.empty:
                return
            coords = SkyCoord(ra=df['ra'], dec=df['dec'], unit='deg')
            df['eb_v'] = self.dustmap_query(coords).astype(np.float32)
            yield df

    def insert_data(self):
        logging.info('Inserting SDF dust map into ClickHouse')
        for df in self.gen_data():
            self.ch_client.client.insert_dataframe(f'INSERT INTO {self.db}.{self.table} VALUES', df)


class DustMapsPutter(CHPutter):
    db = 'dust'

    _available_putters = {'sfd': SFDPutter}

    def __init__(self, dir, tmp_dir, user, host, clickhouse_settings, on_exists, map, **_kwargs):
        self.tmp_dir = tmp_dir or dir
        self.maps = map
        self.putters = tuple(self._available_putters[map](self, tmp_dir) for map in self.maps)
        self.on_exists = on_exists
        self.user = user
        self.host = host
        self.settings = clickhouse_settings
        self.settings['use_numpy'] = True
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

    def create_tables(self, on_exists: str):
        for p in self.putters:
            p.create_table(on_exists)

    def insert_data(self):
        for p in self.putters:
            p.insert_data()

    default_actions = ('create', 'insert',)

    def action_create(self):
        self.create_tables(self.on_exists)

    def action_insert(self):
        self.insert_data()


class DustMapsArgSubParser(ArgSubParser):
    command = 'dust'
    putter_cls = DustMapsPutter

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-m', '--map', default=('sfd',), nargs='+', help='dust maps to put')
