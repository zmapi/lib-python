import csv
import os

def import_err_codes():
    """Populates the module namespace with error names and _errors dict."""
    g = globals()
    errors = g["_errors"] = {}
    script_dir = os.path.dirname(__file__)
    fn = os.path.join(script_dir, "codes", "data", "errcodes.csv")
    with open(fn) as f:
        reader = csv.reader(f, delimiter=",")
        for i, row in enumerate(reader):
            # skip header
            if i == 0:
                continue
            ecode, ename, emsg = row
            errors[int(ecode)] = dict(name=ename, msg=emsg)
            g[ename] = int(ecode)

import_err_codes()

def gen_error(ecode, msg=None):
    assert ecode in _errors
    if not msg:
        msg = _errors[ecode]["msg"]
    return dict(result="error", content=dict(ecode=ecode, msg=msg))

