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
