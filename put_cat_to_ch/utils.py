def subclasses(cls):
    return set(cls.__subclasses__()).union(subcls for c in cls.__subclasses__() for subcls in subclasses(c))
