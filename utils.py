import asyncio
import collections
import inspect
import random
import string
import json
import zmq.asyncio
import os
from numbers import Number
from time import time
from uuid import uuid4
from datetime import datetime
from collections import OrderedDict
from functools import wraps
from zmapi.exceptions import *
from zmapi import fix

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


def check_if_error(msg):
    body = msg["Body"]
    msg_type = msg["Header"]["MsgType"]
    if msg_type == fix.MsgType.Reject:
        raise RejectException(
                body.get("SessionRejectReason"), body.get("Text"))
    if msg_type == fix.MsgType.BusinessMessageReject:
        raise BusinessMessageRejectException(
                body.get("BusinessRejectReason"), body.get("Text"))
    if msg_type == fix.MsgType.MarketDataRequestReject:
        raise MarketDataRequestRejectException(
                body.get("MDReqRejReason"), body.get("Text"))


async def send_recv_command_raw(sock, msg_type, **kwargs):
    body = kwargs.get("body", None)
    timeout = kwargs.get("timeout", None)
    endpoint = kwargs.get("endpoint", None)
    msg = {}
    msg["Header"] = header = {}
    header["MsgType"] = msg_type
    if endpoint:
        header["ZMEndpoint"] = endpoint
    msg["Body"] = body if body is not None else {}
    msg_bytes = (" " + json.dumps(msg)).encode()
    msg_id_in = str(uuid4()).encode()
    await sock.send_multipart([b"", msg_id_in, msg_bytes])
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)
    start_time = time()
    # this will wipe the message queue
    while True:
        if timeout is None:
            res = await poller.poll(timeout)
        else:
            remaining_ms = timeout - (time() - start_time)
            res = await poller.poll(remaining_ms)
        if not res:
            return
        msg_parts = await sock.recv_multipart()
        if msg_parts[-2] == msg_id_in:
            msg = msg_parts[-1]
            break
    msg = json.loads(msg.decode())
    check_if_error(msg)
    return msg


def partition(coll, n, step=None, complete_only=False):
    if step is None:
        step = n
    for i in range(0, len(coll), step):
        items_over = i + n - len(coll)
        if items_over > 0 and complete_only:
            return
        else:
            yield coll[i:i+n]


def get_timestamp():
    return int(datetime.utcnow().timestamp() * 1e9)


async def delayed(f, delay_or_waitable):
    if isinstance(delay_or_waitable, Number):
        await asyncio.sleep(delay_secs)
    else:
        await delay_or_waitable.wait()
    await f()


def makedirs(path):
    if os.path.isdir(path):
        return
    if os.path.isfile(path):
        raise ValueError("'{}' is a file".format(path))
    os.makedirs(path)


def get_zmapi_dir():
    home_dir = os.path.expanduser("~")
    zmapi_dir = os.path.join(home_dir, ".zmapi")
    makedirs(zmapi_dir)
    return zmapi_dir
