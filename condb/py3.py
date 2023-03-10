def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x

def to_bytes(x):
    if isinstance(x, str):
        x = x.encode("utf-8")
    return x
