import logging
import os
from typing import Set


def subclasses(cls: type) -> Set[type]:
    return set(cls.__subclasses__()).union(subcls for c in cls.__subclasses__() for subcls in subclasses(c))


def remove_files_and_directory(dir, files):
    dir = os.path.abspath(dir)

    for f in files:
        path = os.path.abspath(f)
        if dir != os.path.dirname(path):
            raise ValueError(f"File {f} doesn't locate in dir {dir}")

    for f in files:
        os.remove(f)

    try:
        os.rmdir(dir)
    except OSError:  # dir is not empty
        logging.warning(f'dir {dir} is not removed, probably it is not empty')


def _np_dtype_to_ch(dtype: type) -> str:
    dtype = dtype.newbyteorder('<')
    if np.issubdtype(dtype, np.bool_):
        return 'UInt8'
    if np.issubdtype(dtype, np.integer):
        n = 8 * dtype.itemsize
        if np.issubdtype(dtype, np.signedinteger):
            return f'Int{n}'
        return f'UInt{n}'
    if np.issubdtype(dtype, np.floating):
        n = 8 * dtype.itemsize
        return f'Float{n}'
    if np.issubdtype(dtype, np.bytes_):
        n = dtype.itemsize
        return f'FixedString({n})'
    raise ValueError(f"Don't know how to convert {dtype} to ClickHouse column type")


def np_dtype_to_ch(dtype: type, nullable: bool = False) -> str:
    ch_type = _np_dtype_to_ch(dtype)
    if nullable:
        ch_type = f'Nullable({ch_type})'
    return ch_type
