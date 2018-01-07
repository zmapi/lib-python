import zmq
import sys
import logging
import json
from zmapi.codes import error
from zmapi.exceptions import *
from zmapi.zmq.utils import *
from asyncio import ensure_future as create_task
from time import gmtime

L = logging.getLogger(__name__)

class ControllerBase:

    _commands = {}

    def __init__(self, name, ctx, addr):
        self._name = name
        self._tag = "<" + self._name + "> "
        self._sock = ctx.socket(zmq.ROUTER)
        self._sock.bind(addr)

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


    async def _handle_one_2(self, ident, msg):
        cmd = msg["command"]
        L.debug(self._tag + "{}: {}".format(ident_to_str(ident), cmd))
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
