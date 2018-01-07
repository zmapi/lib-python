class RemoteException(Exception):
    
    def __init__(self, ecode, msg):
        self.ecode = ecode
        self.msg = msg
        self.name = _errors.get(ecode)["name"]
        self.args = (self.name, self.msg)

    def __str__(self):
        id = self.name if self.name is not None else self.ecode
        return "RemoteException({}): {}".format(id, self.msg)

class CodecException(Exception):
    pass

class DecodingException(Exception):
    pass

class InvalidArgumentsException(Exception):
    pass

class CommandNotImplementedException(Exception):
    pass
