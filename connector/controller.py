import zmq
import sys
import logging
import json
import aiohttp
import re
from zmapi.codes import error
from zmapi.exceptions import *
from zmapi.zmq.utils import *
from zmapi.utils import *
from zmapi.asyncio import Throttler
from zmapi import SubscriptionDefinition
from asyncio import ensure_future as create_task
from time import time, gmtime
from collections import defaultdict
from copy import deepcopy
from uuid import uuid4

L = logging.getLogger(__name__)

class ControllerBase:

    _commands = {}

    def __init__(self, name, ctx, addr):
        self._name = name
        self._tag = "[" + self._name + "] "
        self._ctx = ctx
        self._sock = ctx.socket(zmq.ROUTER)
        self._sock.bind(addr)
        self._subscriptions = defaultdict(SubscriptionDefinition)

    async def _send_error(self, ident, msg_id, ecode, msg=None):
        msg = error.gen_error(ecode, msg)
        await self._send_reply(ident, msg_id, msg)

    async def _send_result(self, ident, msg_id, content):
        msg = dict(content=content, error=False)
        await self._send_reply(ident, msg_id, msg)
       
    async def _send_reply(self, ident, msg_id, msg):
        msg_bytes = (" " + json.dumps(msg)).encode()
        await self._sock.send_multipart(ident + [b"", msg_id, msg_bytes])

    async def run(self):
        while True:
            msg_parts = await self._sock.recv_multipart()
            # pprint(msg_parts)
            try:
                ident, rest = split_message(msg_parts)
            except ValueError as err:
                L.error(self._tag + str(err))
                continue
            if len(rest) == 1 and not len(rest[0]) == 0:
                # handle ping message
                await self._sock.send_multipart(msg_parts)
                continue
            msg_id, msg = rest
            create_task(self._handle_one_1(ident, msg_id, msg))

    async def _handle_one_1(self, ident, msg_id, msg):
        try:
            msg = json.loads(msg.decode())
            debug_str = "ident={}, command={}, msg_id={}"
            debug_str = debug_str.format(
                    ident_to_str(ident), msg["command"], msg_id)
            L.debug(self._tag + "> " + debug_str)
            res = await self._handle_one_2(ident, msg)
        except InvalidArgumentsException as e:
            L.exception(self._tag + "invalid arguments on message:")
            await self._send_error(ident, msg_id, error.ARGS, str(e))
        except CommandNotImplementedException as e:
            L.exception(self._tag + "command not implemented:")
            await self._send_error(ident, msg_id, error.NOTIMPL, str(e))
        except Exception as e:
            L.exception(self._tag + "general exception handling message:",
                        str(e))
            await self._send_error(ident, msg_id, error.GENERIC, str(e))
        else:
            if res is not None:
                await self._send_result(ident, msg_id, res)
        L.debug(self._tag + "< " + debug_str)

    # It's a required duty for each connector to track it's subscriptions.
    # It's not as good to implement this in a middleware module because
    # middleware lifecycle may not be synchronized with the connector
    # lifecycle.
    async def _handle_modify_subscription(self, ident, msg):
        """Calls modify_subscription and tracks subscriptions."""
        content = msg["content"]
        content_mod = dict(content)
        ticker_id = content_mod.pop("ticker_id")
        old_sub_def = self._subscriptions[ticker_id]
        new_sub_def = deepcopy(old_sub_def)
        new_sub_def.update(content_mod)
        msg_mod = dict(msg)
        msg_mod["content"].update(new_sub_def.__dict__)
        if new_sub_def.empty():
            self._subscriptions.pop(ticker_id, "")
        else:
            self._subscriptions[ticker_id] = new_sub_def
        try:
            res = await self._commands["modify_subscription"](
                    self, ident, msg_mod)
        except Exception as err:
            # revert changes
            self._subscriptions[ticker_id] = old_sub_def
            raise err
        return res

    async def _handle_one_2(self, ident, msg):
        cmd = msg["command"]
        if cmd == "modify_subscription":
            return await self._handle_modify_subscription(ident, msg)
        if cmd == "get_subscriptions":
            return {k: v.__dict__ for k, v in self._subscriptions.items()}
        else:
            f = self._commands.get(cmd)
            if not f:
                raise CommandNotImplementedException(cmd)
            return await f(self, ident, msg)

    @staticmethod
    def handler(cmd=None):
        def decorator(f):
            nonlocal cmd
            if not cmd:
                cmd = f.__name__
            ControllerBase._commands[cmd] = f
            return f
        return decorator


class RESTController(ControllerBase):
    
    """Controller that has built-in throttled and cached http fetching
    capabilities."""

    def __init__(self, name, ctx, addr, throttler_addr=None):
        super().__init__(name, ctx, addr)
        self._rest_result_cache = {}
        self._throttler_regexps = []
        if not throttler_addr:
            throttler_addr = "inproc://throttler-notifications-" + str(uuid4())
        self._throttler_addr = throttler_addr

    async def _http_get_cached(self, url, expiration_s=sys.maxsize, **kwargs):
        session = kwargs.pop("session", None)
        holder = self._rest_result_cache.get(url)
        data = None
        if holder is not None:
            elapsed = time() - holder["timestamp"]
            if elapsed < expiration_s:
                data = holder["data"]
        if data is None:
            # find first throttler matching url and throttle if necessary
            for rex, throttler in self._throttler_regexps:
                if rex.fullmatch(url):
                    await throttler()
                    break
            timestamp = time()
            data = await self._do_http_get(session, url)
            holder = dict(data=data, timestamp=timestamp)
            self._rest_result_cache[url] = holder
        return data

    async def _http_get(self, url, **kwargs):
        return await self._http_get_cached(url, expiration_s=0, **kwargs)

    def _add_throttler(self, regexp, ts_count, tlim_s, tag=None):
        rex = re.compile(regexp)
        sock = self._ctx.socket(zmq.PUB)
        sock.connect(self._throttler_addr)
        if not tag:
            tag = regexp
        throttler = Throttler(ts_count, tlim_s, sock, tag)
        self._throttler_regexps.append((rex, throttler))

    async def _do_http_get(self, session, url):
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        data = None
        async with session.get(url) as r:
            if r.status < 200 or r.status >= 300:
                raise Exception("GET {}: status {}".format(url, r.status))
            data = await r.read()
        if close_session:
            session.close()
        if hasattr(self, "_process_fetched_data"):
            data = self._process_fetched_data(data, url)
        return data
