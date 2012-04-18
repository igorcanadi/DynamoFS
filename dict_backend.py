import cPickle

# Tuple stored in the dict.
class Datum:
    def __init__(self, value):
        self.value = value
        self.refCount = 1

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

    def put(self, key, value):
        if key in self.kvstore:
            self.kvstore[key].refCount += 1
        else:
            self.kvstore[key] = Datum(value) 

    def get(self, key):
        return self.kvstore[key].value

    def incRefCount(self, key):
        self.kvstore[key].refCount += 1
        
    def decRefCount(self, key):
        datum = self.kvstore[key]
        datum.refCount -= 1
        if datum.refCount == 0:
            del self.kvstore[key]
