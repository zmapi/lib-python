import zmq
import zmq.asyncio
import json
import logging
from uuid import uuid4


L = logging.getLogger(__name__)


class ReturningDealer:


    def __init__(self, ctx, sock_dealer):
        self._ctx = ctx
        self._sock_dealer = sock_dealer
        self._pub_addr = "inproc://ReturningDealer" + str(uuid4())
        self._sock_pub = self._ctx.socket(zmq.PUB)
        self._sock_pub.bind(self._pub_addr)
        self._sub_socks = set()
        self._msg_id = 0
        self.running = False


    def __del__(self):
        self.close()


    def close(self):
        self.running = False
        self._sock_dealer.close()
        self._sock_pub.close()
        for sock in self._sub_socks:
            sock.close()


    async def run(self):
        if self.running:
            raise RuntimeError("already running")
        self.running = True
        L.debug("ReturningDealer running: {}".format(self._pub_addr))
        while self.running:
            msg_parts = await self._sock_dealer.recv_multipart()
            msg_id = msg_parts[-2]
            self._sock_pub.send_multipart([msg_id + b"\0"] + msg_parts)
        self._sock_pub.close()


    async def poll_for_msg_id(self, msg_id, timeout=None):
        sock = self._ctx.socket(zmq.SUB)
        self._sub_socks.add(sock)
        sock.connect(self._pub_addr)
        sock.subscribe(msg_id + b"\0")
        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)
        # racing conditions possible here with the destructor?
        res = await poller.poll(timeout)
        if not res:
            sock.close()
            self._sub_socks.remove(sock)
            return
        msg_parts = await sock.recv_multipart()
        sock.close()
        self._sub_socks.remove(sock)
        # drop the topic
        return msg_parts[1:]


    def _gen_msg_id(self):
        msg_id = str(self._msg_id).encode()
        self._msg_id += 1
        return msg_id


    async def send_recv_msg(self, msg : bytes, timeout=None, ident=None):
        msg_id = self._gen_msg_id()
        if ident:
            await self._sock_dealer.send_multipart(ident + [b"", msg_id, msg])
        else:
            await self._sock_dealer.send_multipart([b"", msg_id, msg])
        return await self.poll_for_msg_id(msg_id, timeout)


    # async def destroy(self, timeout=None):
    #     data = {"Header": {"MsgType": fix.MsgType.Heartbeat}}
    #     self.running = False
    #     msg = (" " + json.dumps(data)).encode()
    #     await self.send_recv_msg(msg, timeout)
