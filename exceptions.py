from zmapi import fix


class RejectException(Exception):
    def __init__(self, text, reason=fix.SessionRejectReason.Other):
        super().__init__()
        self.args = (text, reason)


class BusinessMessageRejectException(Exception):
    def __init__(self, text, reason=fix.BusinessRejectReason.ZMGenericError):
        super().__init__()
        self.args = (text, reason)


class MarketDataRequestRejectException(Exception):
    def __init__(self, text, reason=fix.MDReqRejReason.ZMGenericError):
        super().__init__()
        self.args = (text, reason)
