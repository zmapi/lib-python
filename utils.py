import collections
import inspect
import random
import string
from collections import OrderedDict
from functools import wraps
from zmapi.exceptions import *

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


RND_SYMBOLS = string.ascii_uppercase + string.ascii_lowercase + string.digits
def random_str(n, symbols=None):
    if not symbols:
        symbols = RND_SYMBOLS
    return ''.join(random.choice(symbols) for _ in range(n))


async def ctl_send_reply(sock, ident, msg_id, msg):
    if "ZMSendingTime" not in msg["header"]:
        msg["Header"]["ZMSendingTime"] = \
                int(datetime.utcnow().timestamp() * 1e9)
    msg_bytes = (" " + json.dumps(msg)).encode()
    await sock.send_multipart(ident + [b"", msg_id, msg_bytes])


async def ctl_send_xreject(sock, ident, msg_id, msg_type, reason, text):
    d = {}
    d["Header"] = header = {}
    header["MsgType"] = msg_type
    d["Body"] = body = {}
    body["Text"] = text
    if msg_type == fix.MsgType.Reject:
        body["SessionRejectReason"] = reason
    elif msg_type == fix.MsgType.BusinessMessageReject:
        body["BusinessRejectReason"] = reason
    elif msg_type == fix.MsgType.MarketDataRequestReject:
        body["MDReqRejReason"] = reason
    await send_reply(sock, ident, msg_id, d)
