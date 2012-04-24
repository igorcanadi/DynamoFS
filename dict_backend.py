import cPickle
import os
import time

# Tuple stored in the dict.
class Datum:
    def __init__(self, value):
        self.value = value
        self.refCount = 1

# Simple backend that just uses an in-memory dict object, backed by a local file.
class DictBackend:
    # filename is a local file where this dict will persist itself.
    # delayTime is the artificial delay (in seconds) to insert for each backend operation.
    def __init__(self, filename, delayTime = 0.0):
        self.filename = filename
        self.delayTime = delayTime
        try:
            self.kvstore = cPickle.load(open(filename, 'r'))
        except:
            # The file failed to load, so just start with an empty dict.
            self.kvstore = dict()
            
    def delay(self):
        if self.delayTime > 0.0:
            time.sleep(self.delayTime)

    def cleanup(self):
        # Persist the dictionary.
        cPickle.dump(self.kvstore, open(self.filename, 'w'))

    def __del__(self):
        self.cleanup()

    def put(self, key, value):
        self.delay()
        if key in self.kvstore:
            self.kvstore[key].refCount += 1
        else:
            self.kvstore[key] = Datum(value) 

    def get(self, key):
        self.delay()
        return self.kvstore[key].value

    def incRefCount(self, key):
        self.delay()
        self.kvstore[key].refCount += 1
        
    def decRefCount(self, key):
        self.delay()
        datum = self.kvstore[key]
        datum.refCount -= 1
        if datum.refCount == 0:
            del self.kvstore[key]
            
    def nuke(self):
        # Destroy the backing store and revert the in-memory dict.
        try:
            os.unlink(self.filename)
        except OSError:
            pass # The file must have never existed; that's fine.
        
        self.kvstore = dict()
