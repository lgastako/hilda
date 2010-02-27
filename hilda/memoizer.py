from functools import wraps

from collections import defaultdict


MEMO_CACHE = defaultdict(lambda: defaultdict(dict))


def memoize(f):

    @wraps(f)
    def wrapped(self, *args, **kwargs):
        method_cache = MEMO_CACHE[self]
        cache = method_cache[f.__name__]
        key = (args, repr(kwargs))
        if key not in cache:
            cache[key] = apply(f, [self] + list(args), kwargs)
        return cache[key]

    return wrapped


def unmemoize_instance(o):
    MEMO_CACHE[o] = defaultdict(dict)
