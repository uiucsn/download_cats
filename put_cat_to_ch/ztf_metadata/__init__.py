import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.time import Time, TimeDelta
from joblib import Parallel, delayed

from put_cat_to_ch.arg_sub_parser import ArgSubParser
from put_cat_to_ch.shell_runner import ShellRunner
from put_cat_to_ch.putter import CHPutter
from put_cat_to_ch.ztf_metadata import sh, sql

from .fields import get_rcid_centers
from .obstime import exposure_start_to_mid_helio
from .sqlite_query import ZTFMetadataExposures


__all__ = ("ZTFMetadataPutter", "ZTFMetadataArgSubParser",)


class ZTFMetadataPutter(CHPutter):
    # Put to the same DB as ZTF data
    db = "ztf"
    table_name = "exposures"

    def __init__(self, dir, tmp_dir, user, host, clickhouse_settings, on_exists, jobs, **_kwargs):
        self.data_dir = dir
        self.tmp_dir = tmp_dir if tmp_dir is not None else dir
        self.on_exists = on_exists
        self.processes = jobs
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

        self.db_file = Path(self.data_dir) / 'ztf_metadata_latest.db'
        self.tmp_exposure_file = Path(self.tmp_dir) / 'tmp_ztf_metadata.parquet'

    # It is easier to specify columns manually to pack some values to smaller types
    ch_columns = {
        "expid": "UInt32",
        "field": "UInt16",
        "filter": "Enum('g', 'r', 'i')",
        "ra": "Float64",
        "dec": "Float64",
        "exptime": "Float32",
        "airmass": "Float32",
        "infobits": "Nullable(UInt8)",  # They all zero for some reason
        "dr": "Nullable(UInt8)",
        "numsci": "Nullable(UInt8)",  # 0 to 64
        "numdiff": "Nullable(UInt8)",  # They all zero for some reason
        "fwhm": "Nullable(Float32)",
        "maglim": "Nullable(Float32)",
        "scibckgnd": "Nullable(Float32)",
        "ellip": "Nullable(Float32)",
        "ellippa": "Nullable(Float32)",
        # Few columns we are going to add
        "expstart_mjd": "Float64",
        "expmid_hmjd": "Float64",
        "rcid": "UInt8",
    }

    @property
    def ch_columns_str(self) -> str:
        return ",\n    ".join(f"{name} {ch_type}" for name, ch_type in self.ch_columns.items())

    def create_table(self, on_exists: str):
        logging.info('Creating DB table for ZTF exposure metadata')
        exists_ok = self.process_on_exists(on_exists, self.db, self.table_name)
        self.exe_query(
            'create_table.sql',
            if_not_exists=self.if_not_exists(exists_ok),
            db=self.db,
            table=self.table_name,
            columns=self.ch_columns_str,
        )

    def prepare_data(self):
        logging.info('Preparing ZTF exposure metadata')

        logging.info('Getting ZTF fields centers')
        # Convert rcid from index to column
        rcid_centers = get_rcid_centers().reset_index(level=1)

        logging.info('Getting ZTF exposure metadata')
        with ZTFMetadataExposures(path=self.db_file) as ztf_metadata:
            ch_columns_wo_new = {name: ch_type for name, ch_type in self.ch_columns.items() if name not in {'expstart_mjd', 'expmid_hmjd', 'rcid'}}
            ztf_metadata.validate_ch_columns(ch_columns_wo_new)
            # Here ra and dec are for the center of the field
            exposures = ztf_metadata.get_data(exclude=['ra', 'dec'])

        logging.info('Merging ZTF exposure metadata with field centers')
        df = pd.merge(exposures, rcid_centers, how='inner', left_on='field', right_on='fieldid')

        # Looks like a bug in astropy, it cannot convert it from dtype=object or string[python]
        obsdate = Time(np.asarray(df['obsdate'], dtype=str))
        del df['obsdate']
        df['expstart_mjd'] = obsdate.mjd

        exptime = TimeDelta(df['exptime'], format='sec')
        coord = SkyCoord(df['ra'], df['dec'], unit='deg')

        logging.info(f'Calculating exposure mid time in heliocentric MJD, in parallel with {self.processes} processes')
        step = len(df) // (self.processes + 1)
        edges = np.arange(0, len(df) + step, step)
        result_times = Parallel(n_jobs=self.processes, backend="loky")(delayed(exposure_start_to_mid_helio)(obsdate[start:end], exptime[start:end], coord[start:end]) for start, end in zip(edges[:-1], edges[1:]))
        mjd = np.concatenate([times.mjd for times in result_times])
        df['expmid_hmjd'] = mjd

        df.to_parquet(self.tmp_exposure_file, index=False)

        return df

    def insert_data(self):
        logging.info('Inserting ZTF exposure metadata into ClickHouse')
        if not self.tmp_exposure_file.exists():
            raise FileNotFoundError(f'File {self.tmp_exposure_file} does not exist')
        self.shell_runner(
            'insert_parquet_file.sh',
            str(self.tmp_exposure_file),
            f'{self.db}.{self.table_name}',
            self.host,
        )

    default_actions = ('create', 'tmp_parquet', 'insert', 'exposures_field')

    def action_create(self):
        self.create_db(self.db)
        self.create_table(self.on_exists)

    def action_tmp_parquet(self):
        self.prepare_data()

    def action_insert(self):
        self.insert_data()

    def action_exposures_field(self):
        self.exe_query(
            'exposures_field.sql',
            db=self.db,
            table=self.table_name,
            db_exposures=self.db,
            table_exposures=f'{self.table_name}_field',
        )


class ZTFMetadataArgSubParser(ArgSubParser):
    command = 'ztf_metadata'
    putter_cls = ZTFMetadataPutter

    def __init__(self, cli_args: argparse.Namespace):
        super().__init__(cli_args)

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser):
        super().add_arguments_to_parser(parser)
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help='number of jobs for tmp_parquet action')
