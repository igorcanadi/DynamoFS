import cPickle
import key_hash

# Simple backend that just uses an in-memory dict object, backed by a local file.
class DictBackend:
    # filename is a local file where this dict will persist itself.
    def __init__(self, filename):
        self.filename = filename
        try:
            self.kvstore = cPickle.load(open(filename, 'r'))
        except:
            # The file failed to load, so just start with an empty dict.
            self.kvstore = dict()
            pass

    def __del__(self):
        # Persist the dictionary.
        cPickle.dump(self.kvstore, open(self.filename, 'w'))

    def put(self, value):
        key = key_hash.generateKey(value)
        self.kvstore[key] = value
        self.incRefCount(key)

    def get(self, key):
        return self.kvstore[key]

    def incRefCount(self, key):
        pass # We don't do garbage collection for now.
        
    def decRefCount(self, key):
        pass # We don't do garbage collection for now.