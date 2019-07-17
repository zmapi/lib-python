from .utils import get_timestamp, makedirs, check_if_error
from .exceptions import *
import pickle
import os
import shutil
import re
import copy
import logging
import json
from collections import defaultdict
from base64 import b64encode, b64decode
from pprint import pprint, pformat


###############################################################################


L = logging.getLogger(__name__)


###############################################################################



class Publisher:


    def __init__(self, sock, **kwargs):
        self._sock = sock
        self._msg_cache_dir = kwargs.pop("msg_cache_dir", None)
        self._max_buffer_bytes = kwargs.pop("max_buffer_bytes", 10_000_000)
        if kwargs:
            raise ValueError(f"unsupported kwargs: {kwargs}")
        def create_empty_state():
            return {
                "seq_num": 1,
                "msg_buffer": [],
                "buffer_bytes": 0,
            }
        self._state = defaultdict(create_empty_state)
        if self._msg_cache_dir:
        #     try:
        #         shutil.rmtree(self._msg_cache_dir)
        #     except:
        #         pass
            os.makedirs(self._msg_cache_dir)


    def _save_and_clear_msg_buffer(self, topic, state):
        if not self._msg_cache_dir:
            raise RuntimeError("message cache is disabled")
        msg_buffer = state["msg_buffer"]
        seq_num_start = msg_buffer[0][0]
        seq_num_end = msg_buffer[-1][0]
        topic_str = "" if topic is None else topic.strip(b"\0").decode()
        fn = f"messages_({topic_str})_{seq_num_start}_{seq_num_end}.p"
        fn = os.path.join(self._msg_cache_dir, fn)
        with open(fn, "wb") as f:
            pickle.dump(msg_buffer, f)
        msg_buffer.clear()
        state["buffer_bytes"] = 0


    def _save_msg(self, state, seq_num, topic, msg, msg_bytes):
        if not self._msg_cache_dir:
            raise RuntimeError("message cache is disabled")
        state["msg_buffer"].append((seq_num, msg))
        # include only msg_bytes len in this calculation
        state["buffer_bytes"] += len(msg_bytes)
        # L.debug("buffer bytes: {}".format(self._buffer_bytes))
        if state["buffer_bytes"] >= self._max_buffer_bytes:
            self._save_and_clear_msg_buffer(topic, state)


    async def _send_msg(self, topic : bytes, msg : bytes):
        if topic is None:
            msg_parts = [msg]
        else:
            msg_parts = [topic, msg]
        await self._sock.send_multipart(msg_parts)


    def fetch_messages(self, start, end, topic):
        if not self._msg_cache_dir:
            raise NotImplementedError("message cache is disabled")
        L.debug(f"fetch messages: {start} {end}")
        state = self._state[topic.encode()]
        msg_buffer = state["msg_buffer"]
        if msg_buffer:
            buffer_start = msg_buffer[0][0]
            buffer_end = msg_buffer[-1][0]
            if end == 0:
                end = buffer_end
            assert start >= 1, (start, end)
            assert end >= start, (start, end)
            assert end <= buffer_end, (end, buffer_end)
            if start > buffer_start:
                offset = buffer_start - start
                res = msg_buffer[offset:offset+(end-start+1)]
                return copy.deepcopy(res)
        res = None
        for x in os.listdir(self._msg_cache_dir):
            fn = os.path.join(self._msg_cache_dir, x)
            try:
                r = re.match(r"(messages_\()(.*?)(\)_)(\d+)(_)(\d+)", x).groups()
            except:
                continue
            f_topic = r[1]
            if f_topic != topic:
                continue
            f_start = int(r[3])
            f_end = int(r[5])
            if f_start <= start <= f_end:
                with open(fn, "rb") as f:
                    res = pickle.load(f)
                    break
        if res is None:
            raise Exception("no matching data found from msg_cache_dir")
        if end <= f_end:
            offset = start - f_start
            return res[offset:offset+(end-start+1)]
        res += self.fetch_messages(f_end + 1, end, topic)
        return res


    async def republish(self, start, end, topic, req_id):
        if type(topic) is bytes:
            topic = topic.decode()
        for _, msg in self.fetch_messages(start, end, topic):
            msg["Header"]["PossDupFlag"] = True
            msg["Body"]["ZMReqID"] = req_id
            msg_bytes = (" " + json.dumps(msg)).encode()
            pub_topic = req_id.encode() + b"\0" + topic.encode() + b"\0"
            await self._send_msg(pub_topic, msg_bytes)
            #await self._sock.send_multipart([pub_topic, msg_bytes])


    async def publish(
            self, msg_type, body=None, topic: bytes = None, no_seq_num=False):
        if body is None:
            body = {}
        msg = {}
        msg["Header"] = {"MsgType": msg_type}
        msg["Body"] = body
        return await self.publish_msg(msg, topic=topic, no_seq_num=no_seq_num)


    # mutates msg
    async def publish_msg(
            self, msg: dict, topic: bytes = None, no_seq_num=False):
        if topic != None and topic[-1] != 0:
            # append null byte to topic if necessary
            topic += b"\0"
        if "ZMSendingTime" not in msg["Header"]:
            msg["Header"]["ZMSendingTime"] = get_timestamp()
        state = self._state[topic]
        if not no_seq_num:
            msg["Header"]["MsgSeqNum"] = seq_num = state["seq_num"]
            state["seq_num"] += 1
        msg_bytes = (" " + json.dumps(msg)).encode()
        # if topic is None:
        #     msg_parts = [msg_bytes]
        # else:
        #     msg_parts = [topic, msg_bytes]
        if self._msg_cache_dir and not no_seq_num:
            self._save_msg(state, seq_num, topic, msg, msg_bytes)
        await self._send_msg(topic, msg_bytes)
        #await self._sock.send_multipart(msg_parts)
        return msg["Header"].get("MsgSeqNum")


###############################################################################


class Subscriber:


    def __init__(self, sock, dealer=None, name=None):
        self._sock_sub = sock
        self._sock_dealer = dealer
        self._name = name
        if self._name:
            self._tag = "[" + self._name + "] "
        else:
            self._tag = ""
        def create_empty_state():
            return {
                "expected_seq_no": None
            }
        self._state = defaultdict(create_empty_state)
        self._session_id = None
        self._expected_session_id = None


    async def _handle_msg_2(self, topic, msg):
        raise NotImplementedError("_handle_msg_2 must be implemented")


    async def _handle_msg_1(self, topic, msg):
        try:
            await self._handle_msg_2(topic, msg)
        except Exception:
            L.exception("error in Subscriber._handle_msg_2:")


    async def _on_seq_no_reset(self):
        pass


    async def _send_recv_command(self, msg_type, **kwargs):
        body = kwargs.get("body", {})
        ident = kwargs.get("ident", None)
        timeout = kwargs.get("timeout", None)
        endpoint = kwargs.get("endpoint", None)
        check_error = kwargs.get("check_error", False)
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = msg_type
        if endpoint:
            header["ZMEndpoint"] = endpoint
        msg["Body"] = body
        msg_bytes = (" " + json.dumps(msg)).encode()
        msg_parts = await self._sock_dealer.send_recv_msg(
                msg_bytes, ident=ident, timeout=timeout)
        if not msg_parts:
            return
        msg = json.loads(msg_parts[-1].decode())
        if check_error:
            check_if_error(msg)
        return msg


    async def _req_gap_fill(self, start, end, topic):
        body = {}
        body["BeginSeqNo"] = start
        body["EndSeqNo"] = end
        body["ZMPubTopic"] = topic
        res = await self._send_recv_command(
                fix.MsgType.ResendRequest, body=body, check_error=True)
        body = res["Body"]
        if body["ZMSessionID"] != self._expected_session_id:
            L.info(self._tag + "session changed during gap, "
                   "requesting gap fill from the beginning ...")
            self._expected_session_id = body["ZMSessionID"]
            return self._req_gap_fill(1, end)
        messages = [json.loads(b64decode(x.encode()).decode())
                    for x in body["ZMNoPubMessages"]]
        # if "ZMNoSubscriberTopics" in body:
        #     topics = [b64decode(x.encode()).decode()
        #               for x in body["ZMNoSubscriberTopics"]]
        # else:
        #     topics = [None] * len(messages)
        return messages


    async def _get_session_id(self):
        res = await self._send_recv_command(
                fix.MsgType.ZMGetSessionID, check_error=True)
        return res["Body"]["ZMSessionID"]


    async def handle_one(self):
        if self._expected_session_id is None:
            self._expected_session_id = await self._get_session_id()
            L.debug(self._tag + "upstream session id: {}"
                    .format(self._expected_session_id))
        msg_parts = await self._sock_sub.recv_multipart()
        # message must be the last part of the message
        msg = json.loads(msg_parts[-1].decode())
        topic = None
        if len(msg_parts) > 1:
            # topic must be the second last part of the message
            topic = msg_parts[-2].strip(b"\0").decode()
        state = self._state[topic]
        header = msg["Header"]
        seq_no = header.get("MsgSeqNum")
        if seq_no is None:
            await self._handle_msg_1(topic, msg)
            return
        if state["expected_seq_no"] is None:
            state["expected_seq_no"] = seq_no
        if seq_no == state["expected_seq_no"]:
            state["expected_seq_no"] += 1
            await self._handle_msg_1(topic, msg)
            return
        if seq_no < state["expected_seq_no"]:
            if header.get("PossDupFlag"):
                return
            L.info(self._tag + "MsgSeqNum smaller than expected")
            await self._on_seq_no_reset()
            if seq_no == 1:
                state["expected_seq_no"] = 2
                await self._handle_msg_1(topic, msg)
                return
            state["expected_seq_no"] = 1
        assert seq_no > state["expected_seq_no"], locals()
        start = state["expected_seq_no"]
        end = seq_no - 1
        state["expected_seq_no"] = seq_no + 1
        L.warning("{}gap detected: {}-{}".format(self._tag, start, end))
        if self._session_id:
            self.info(self._tag + "requesting gap fill ...")
            try:
                messages = await self._req_gap_fill(
                        start, end, topic)
            except Exception:
                L.error("error on Subscriber._req_gap_fill:")
            else:
                for t, m in zip(topics, messages):
                    await self._handle_msg_1(t, m)
        await self._handle_msg_1(topic, msg)


    async def run(self):
        L.debug(self._tag + "running ...")
        while True:
            await self.handle_one()

