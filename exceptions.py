from zmapi import fix


class RejectException(Exception):

    """
    Exception wnapper for ZMReject.

    error_condition: Hint that tells whether this should be considered an
                     actual error rather than a failed test.
    """


    def __init__(self,
                 text,
                 reason=fix.ZMRejectReason.Other,
                 field_name=None,
                 error_condition=True):
        super().__init__()
        self.args = (text, reason, field_name)
        self.error_condition = error_condition
