""" 
Type 0 - Directory Blob
     data = {"children": {...}}
Type 1 - Block List Blob
     data = {"blocks": [...]}
Type 2 - Block Blob
     data = {"data": "..."}
""" 

import cPickle
import hashlib

class Blob:
    type = -1
    data = dict()

    def __init__(self, blob):
        (self.type, self.data) = cPickle.loads(blob)

    @classmethod
    def generate_from_type(c, type):
        data = dict()

        if type == 0:
            data["children"] = {}
        elif type == 1:
            data["blocks"] = []
        elif type == 2:
            data["data"] = ""
        else:
            raise Exception('wrong type dude!')

        return c(cPickle.dumps((type, data)))

    # returns (hash, blob)
    def get_hash_and_blob(self):
        cp = cPickle.dumps((self.type, self.data))
        return (hashlib.sha512(cp).hexdigest(), cp)

