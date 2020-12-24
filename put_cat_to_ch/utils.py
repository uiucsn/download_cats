from typing import Set


def subclasses(cls: type) -> Set[type]:
    return set(cls.__subclasses__()).union(subcls for c in cls.__subclasses__() for subcls in subclasses(c))


class Everything:
    def __contains__(self, value):
        return True

    def __repr__(self):
        return 'Everything'
