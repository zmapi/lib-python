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

L = logging.getLogger(__name__)

class ControllerBase:

    _commands = {}

    def __init__(self, name, ctx, addr):
        self._name = name
        self._tag = "[" + self._name + "] "
        self._sock = ctx.socket(zmq.ROUTER)
        self._sock.bind(addr)
        self._subscriptions = {}

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
    async def _handle_subscribe(self, ident, msg):
        """Calls subscribe and tracks subscriptions."""
        required_fields = [
            "ticker_id",
            "order_book_speed",
            "trades_speed",
            "order_book_levels"
        ]
        check_missing(required_fields, msg["content"])
        content = msg["content"]
        ticker_id = content["ticker_id"]
        ob_speed = content["order_book_speed"]
        trades_speed = content["trades_speed"]
        ob_levels = content["order_book_levels"]
        if ob_speed < 0 or ob_speed > 10:
            raise InvalidArgumentsException(
                    "order_book_speed must be in range [0, 10]")
        if trades_speed < 0 or trades_speed > 10:
            raise InvalidArgumentsException(
                    "trades_speed must be in range [0, 10]")
        if ob_levels < 0:
            raise InvalidArgumentsException(
                    "order_book_levels must be a positive integer")
        res = await self._commands["subscribe"](self, ident, msg)
        if ob_speed == 0 and trades_speed == 0 and ob_levels == 0:
            try:
                del self._subscriptions[ticker_id]
            except KeyError:
                pass
        else:
            sub_def = {
                "order_book_speed": ob_speed,
                "trades_speed": trades_speed,
                "order_book_levels": ob_levels
            }
            self._subscriptions[ticker_id] = sub_def
        return res

    async def _handle_one_2(self, ident, msg):
        cmd = msg["command"]
        if cmd == "subscribe":
            return await self._handle_subscribe(ident, msg)
        if cmd == "get_subscriptions":
            return self._subscriptions
        else:
            f = self._commands.get(cmd)
            if not f:
                raise CommandNotImplemented(cmd)
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
