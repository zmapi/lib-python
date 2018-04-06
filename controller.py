import zmq
import sys
import logging
import json
import aiohttp
import re
import string
from datetime import datetime
from zmapi.exceptions import *
from zmapi.zmq.utils import *
from zmapi.asyncio import Throttler
from asyncio import ensure_future as create_task
from time import time, gmtime
from collections import defaultdict
from copy import deepcopy
from uuid import uuid4
from zmapi import fix


L = logging.getLogger(__name__)


class Controller:


    def __init__(self, sock_dn, name=None):
        self._name = name
        if self._name:
            self._tag = "[" + self._name + "] "
        else:
            self._tag = ""
        self._sock_dn = sock_dn
        self._commands = {}
        # subclass to superclass order
        mro = [x for x in self.__class__.mro()
               if issubclass(x, Controller)]
        for name, msg_type in fix.MsgType.__dict__.items():
            if name[0] not in string.ascii_uppercase:
                continue
            for klass in mro:
                f = klass.__dict__.get(name)
                if f:
                    self._commands[msg_type] = f
                    break


    async def _send_reply(self, ident, msg_id, msg):
        if type(msg) == bytes:
            msg_bytes = msg
        else:
            if "ZMSendingTime" not in msg["Header"]:
                msg["Header"]["ZMSendingTime"] = \
                        int(datetime.utcnow().timestamp() * 1e9)
            msg_bytes = (" " + json.dumps(msg)).encode()
        await self._sock_dn.send_multipart(ident + [b"", msg_id, msg_bytes])


    async def _send_xreject(self, ident, msg_id, msg_type, reason, text):
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
        await self._send_reply(ident, msg_id, d)


    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        raise NotImplementedError("_handle_msg_2 must be implemented")


    async def _handle_msg_1(self, ident, msg_id, msg_raw):
        try:
            msg = json.loads(msg_raw.decode())
            msg_type = msg["Header"]["MsgType"]
            debug_str = "ident={}, MsgType={}, msg_id={}"
            debug_str = debug_str.format(
                    ident_to_str(ident), msg_type, msg_id)
            L.debug(self._tag + "> " + debug_str)
            res = await self._handle_msg_2(
                    ident, msg_raw, msg, msg_type)
        except RejectException as e:
            L.exception(self._tag + "Reject processing {}: {}"
                        .format(msg_id, str(e)))
            reason, text = e.args
            await self._send_xreject(ident,
                                     msg_id,
                                     fix.MsgType.Reject,
                                     reason,
                                     text)
        except BusinessMessageRejectException as e:
            L.exception(self._tag + "BMReject processing {}: {}"
                        .format(msg_id, str(e)))
            reason, text = e.args
            await self._send_xreject(ident,
                                     msg_id,
                                     fix.MsgType.BusinessMessageReject,
                                     reason,
                                     text)
        except MarketDataRequestRejectException as e:
            L.exception(self._tag + "MDRReject processing {}: {}"
                        .format(msg_id, str(e)))
            reason, text = e.args
            await self._send_xreject(ident,
                                     msg_id,
                                     fix.MsgType.MarketDataRequestReject,
                                     reason,
                                     text)
        except Exception as e:
            L.exception(self._tag + "GenericError processing {}: {}"
                        .format(msg_id, str(e)))
            await self._send_xreject(ident,
                                     msg_id,
                                     fix.MsgType.BusinessMessageReject,
                                     fix.BusinessRejectReason.ZMGenericError,
                                     "{}: {}".format(type(e).__name__, e))
        else:
            if res is not None:
                await self._send_reply(ident, msg_id, res)
        L.debug(self._tag + "< " + debug_str)


    async def run(self):
        L.debug(self._tag + "controller running ...")
        while True:
            msg_parts = await self._sock_dn.recv_multipart()
            try:
                ident, rest = split_message(msg_parts)
            except ValueError as err:
                L.error(str(err))
                continue
            msg_id, msg = rest
            create_task(self._handle_msg_1(ident, msg_id, msg))


############################ CONNECTOR CONTROLLERS ############################


class ConnectorCTL(Controller):


    def __init__(self, sock_dn, name=None):
        super().__init__(sock_dn, name=name)
        self._subscriptions = {}
        self.insid_to_tid = {}
        self._ticker_id = 0


    def gen_ticker_id(self):
        tid = self._ticker_id
        self._ticker_id += 1
        return tid


    # It's a required duty for each connector to track it's subscriptions.
    # It's not as good to implement this in a middleware module because
    # middleware lifecycle may not be synchronized with the connector
    # lifecycle.
    async def _handle_market_data_request(self, ident, msg_raw, msg):
        sub_def = deepcopy(msg["Body"])
        sub_def.pop("MDReqID", None)
        instrument_id = sub_def["ZMInstrumentID"]
        if instrument_id not in self.insid_to_tid:
            self.insid_to_tid[instrument_id] = self.gen_ticker_id()
        tid = self.insid_to_tid[instrument_id]
        old_sub_def = self._subscriptions.get(instrument_id)
        if sub_def["SubscriptionRequestType"] == '2':
            self._subscriptions.pop(tid, None)
        else:
            self._subscriptions[tid] = sub_def
        try:
            res = await self.MarketDataRequest(ident, msg_raw, msg)
            res["Body"]["ZMTickerID"] = tid
        except Exception as e:
            self._subscriptions[instrument_id] = old_sub_def
            raise e
        return res


    async def _handle_security_list_request(self, ident, msg_raw, msg):
        if "SecurityListRequest" not in self.__class__.__dict__:
            raise BusinessMessageRejectException(
                    fix.BusinessRejectReason.UnsupportedMessageType,
                    "MsgType '{}' not supported".format(msg_type))
        res = await self.SecurityListRequest(ident, msg_raw, msg)
        body = res["Body"]
        for d in body["SecListGrp"]:
            insid = d["ZMInstrumentID"]
            if insid not in self.insid_to_tid:
                self.insid_to_tid[insid] = self.gen_ticker_id()
            tid = self.insid_to_tid[insid]
            d["ZMTickerID"] = tid
        return res


    async def _handle_list_directory(self, ident, msg_raw, msg):
        if "ZMListDirectory" not in self.__class__.__dict__:
            raise BusinessMessageRejectException(
                    fix.BusinessRejectReason.UnsupportedMessageType,
                    "MsgType '{}' not supported".format(msg_type))
        res = await self.ZMListDirectory(ident, msg_raw, msg)
        body = res["Body"]
        for d in body["ZMDirEntries"]:
            insid = d.get("ZMInstrumentID")
            if insid:
                if insid not in self.insid_to_tid:
                    self.insid_to_tid[insid] = self.gen_ticker_id()
                tid = self.insid_to_tid[insid]
                d["ZMTickerID"] = tid
        return res


    async def TestRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.Heartbeat}
        tr_id = body.get("TestReqID")
        res["Body"] = {}
        if tr_id:
            res["Body"]["TestReqID"] = tr_id
        return res


    async def ZMGetSubscriptions(self, ident, msg_raw, msg):
        body = msg["Body"]
        tid = body.get("ZMTickerID")
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMGetSubscriptionsResponse}
        res["Body"] = body = {}
        if tid:
            d = {"ZMTickerID": tid, "ZMSubscription": self._subscriptions[tid]}
            body["ZMSubscriptionsGrp"] = [d]
        else:
            body["ZMSubscriptionsGrp"] = \
                    [{"ZMTickerID": k, "ZMSubscription": v}
                     for k, v in self._subscriptions.items()]
        return res


    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        if msg_type == fix.MsgType.MarketDataRequest:
            return await self._handle_market_data_request(
                    ident, msg_raw, msg)
        elif msg_type == fix.MsgType.SecurityListRequest:
            return await self._handle_security_list_request(
                    ident, msg_raw, msg)
        elif msg_type == fix.MsgType.ZMListDirectory:
            return await self._handle_list_directory(
                    ident, msg_raw, msg)
        else:
            f = self._commands.get(msg_type)
            if not f:
                raise BusinessMessageRejectException(
                        fix.BusinessRejectReason.UnsupportedMessageType,
                        "MsgType '{}' not supported".format(msg_type))
            return await f(self, ident, msg_raw, msg)


    # async def _handle_msg_2(self, ident, msg_id, msg, msg_type):
    #     # if msg_type == fix.MsgType.TestRequest:
    #     #     return await self._handle_test_request(ident, msg)
    #     # if msg_type == fix.MsgType.MarketDataRequest:
    #     #     return await self._handle_market_data_request(ident, msg)
    #     # if msg_type == fix.MsgType.ZMGetSubscriptions:
    #     #     pass
    #     #     # return {k: v.__dict__ for k, v in self._subscriptions.items()}
    #     # else:


class RESTConnectorCTL(ConnectorCTL):
    
    """Controller that has built-in throttled and cached http fetching
    capabilities."""


    def __init__(self, sock_dn, ctx, throttler_addr=None, name=None):
        super().__init__(sock_dn, name=name)
        self._ctx = ctx
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
