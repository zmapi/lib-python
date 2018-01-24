from zmapi.exceptions import *
from functools import wraps
import collections
from collections import OrderedDict
import inspect

def check_missing(fields, d):
    if type(fields) is str:
        fields = [fields]
    for field in fields:
        if field not in d:
            raise InvalidArgumentsException("missing field: {}"
                                            .format(field))


# Modified version of https://gist.github.com/jaredlunde/7a118c03c3e9b925f2bf
def lru_cache(maxsize=128):
    cache = OrderedDict()
    def decorator(fn):
        @wraps(fn)
        async def memoizer(*args, **kwargs):
            key = str((args, kwargs))
            try:
                cache[key] = cache.pop(key)
            except KeyError:
                if len(cache) >= maxsize:
                    cache.popitem(last=False)
                if inspect.iscoroutinefunction(fn):
                    cache[key] = await fn(*args, **kwargs)
                else:
                    cache[key] = fn(*args, **kwargs)
            return cache[key]
        return memoizer
    return decorator

# copied from https://stackoverflow.com/a/3233356/1793556
def update_dict(d, u):
    """Update dict recursively.
    
    Mutates dict d.
    """
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d
