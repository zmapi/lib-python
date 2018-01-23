import zmq
import sys
import logging
import json
from zmapi.codes import error
from zmapi.exceptions import *
from zmapi.zmq.utils import *
from zmapi.utils import *
from asyncio import ensure_future as create_task
from time import gmtime
from collections import defaultdict

L = logging.getLogger(__name__)

class ControllerBase:

    _commands = {}

    def __init__(self, name, ctx, addr):
        self._name = name
        self._tag = "[" + self._name + "] "
        self._sock = ctx.socket(zmq.ROUTER)
        self._sock.bind(addr)
        self._subscriptions = defaultdict(empty_sub_def)

    async def _send_error(self, ident, msg_id, ecode, msg=None):
        msg = error.gen_error(ecode, msg)
        msg["msg_id"] = msg_id
        await self._send_reply(ident, msg)

    async def _send_result(self, ident, msg_id, content):
        msg = dict(msg_id=msg_id, content=content, result="ok")
        await self._send_reply(ident, msg)
       
    async def _send_reply(self, ident, res):
        msg = " " + json.dumps(res)
        msg = msg.encode()
        await self._sock.send_multipart(ident + [b"", msg])

    async def run(self):
        while True:
            msg_parts = await self._sock.recv_multipart()
            # pprint(msg_parts)
            try:
                ident, msg = split_message(msg_parts)
            except ValueError as err:
                L.error(self._tag + str(err))
                continue
            if len(msg) == 0:
                # handle ping message
                await self._sock.send_multipart(msg_parts)
                continue
            create_task(self._handle_one_1(ident, msg))

    async def _handle_one_1(self, ident, msg):
        try:
            msg = json.loads(msg.decode())
            msg_id = msg.get("msg_id")
            debug_str = "ident={}, command={}, msg_id={}"
            debug_str = debug_str.format(
                    ident_to_str(ident), msg["command"], msg["msg_id"])
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
        new_sub_def = dict(old_sub_def)
        new_sub_def.update(content_mod)
        msg_mod = dict(msg)
        msg_mod["content"].update(new_sub_def)
        if sub_def_is_empty(new_sub_def):
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
            return dict(self._subscriptions)
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
