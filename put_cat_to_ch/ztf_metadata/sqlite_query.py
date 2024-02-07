import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class SqlLiteColumnInfo:
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int

    def __post_init__(self):
        self.null = not self.notnull


class ZTFMetadataExposures:
    table_name: str = 'exposures'

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.conn = None
        self.cursor = None

    @staticmethod
    def _get_table_info(cursor, table_name: str) -> dict[str, SqlLiteColumnInfo]:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {row[1]: SqlLiteColumnInfo(*row) for row in cursor.fetchall()}

    def __enter__(self):
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()

        self.table_info = self._get_table_info(self.cursor, self.table_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def validate_ch_columns(self, ch_columns: dict[str, str]):
        for name, ch_type in ch_columns.items():
            if name not in self.table_info:
                raise ValueError(f"Column {name} not found in {self.table_name}")
            if ch_type.startswith('Nullable') ^ self.table_info[name].null:
                raise ValueError(f"Column {name} nullability mismatch")

    def get_data(self, include: list[str] | None = None, exclude: list[str] | None = None):
        """Get data from the table

        Parameters
        ----------
        include : list[str] | None
            List of columns to include. If None, all columns are included
        exclude : list[str] | None
            List of columns to exclude. If None, no columns are excluded
        """
        if include is None:
            include = list(self.table_info.keys())
        if exclude is not None:
            exclude_set = set(exclude)
            include = [col for col in include if col not in exclude_set]

        selectors = []
        for col in include:
            if col not in self.table_info:
                raise ValueError(f"Column {col} not found in {self.table_name}")

            selector = col
            if self.table_info[col].type != 'TEXT' and self.table_info[col].notnull:
                selector = f"NULLIF({col}, '')"

            selectors.append(selector)

        self.cursor.execute(f"SELECT {', '.join(selectors)} FROM {self.table_name}")
        data = self.cursor.fetchall()

        df = pd.DataFrame(data, columns=include)
        return df
