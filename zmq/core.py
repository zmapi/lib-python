import zmq
import zmq.asyncio
import uuid
import json

class SockRecvPublisher:

    def __init__(self, ctx, sock_listen):
        self._ctx = ctx
        self._sock_listen = sock_listen
        self._pub_addr = "inproc://SockRecvPublisher" + str(uuid.uuid4())
        self._sock_pub = self._ctx.socket(zmq.PUB)
        self._sock_pub.bind(self._pub_addr)
        self._running = True

    async def run(self):
        while self._running:
            msg_parts = await self._sock_listen.recv_multipart()
            msg = msg_parts[-1]
            if len(msg) == 0:
                # Control message topics have no leading space.
                topic = b"PONG"
            else:
                msg = json.loads(msg.decode())
                # Normal msg_ids are given own topic prefixed with space to
                # separate them from control message topics.
                topic = b" " + msg["msg_id"].encode()
            self._sock_pub.send_multipart([topic] + msg_parts)
        self._sock_pub.close()

    async def poll_for_msg_id(self, msg_id : str, timeout=None):
        topic = " " + msg_id
        topic = topic.encode()
        return await self._poll_for_topic(topic, timeout)

    async def poll_for_pong(self, timeout=None):
        return await self._poll_for_topic(b"PONG", timeout)

    async def _poll_for_topic(self, topic : bytes, timeout):
        sock = self._ctx.socket(zmq.SUB)
        sock.connect(self._pub_addr)
        sock.subscribe(topic)
        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)
        res = await poller.poll(timeout)
        if not res:
            return
        msg_parts = await sock.recv_multipart()
        sock.close()
        # drop the topic
        return msg_parts[1:]

    async def destroy(self):
        self._sock_listen.send_multipart([b"", b""])
        self._running = False
        await self.poll_for_pong()
