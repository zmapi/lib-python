import zmq

def split_message(msg_parts):
    separator_idx = None
    for i, part in enumerate(msg_parts):
        if not part:
            separator_idx = i
            break
    if not separator_idx:
        raise ValueError("ident separator not found")
    ident = msg_parts[:separator_idx]
    rest = msg_parts[separator_idx+1:]
    return ident, rest

def ident_to_str(ident):
    # "latin-1" decoding never throws exceptions on python on any input so 
    # something printable will always come out as a result...
    return "/".join([x.decode("latin-1").replace("/", "\/") for x in ident])

def get_last_ep(sock):
    return sock.getsockopt_string(zmq.LAST_ENDPOINT)
