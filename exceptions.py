from zmapi import fix

class RejectException(Exception):
    def __init__(self,
                 text,
                 reason=fix.SessionRejectReason.Other,
                 field_name=None):
        super().__init__()
        self.args = (text, reason, field_name)
