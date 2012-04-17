import hashlib

def generateKey(value):
    # TODO tweak the hash size.
    return hashlib.sha512(value).hexdigest()