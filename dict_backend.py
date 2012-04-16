import cPickle
import hashlib

# Simple backend that just uses an in-memory dict object, backed by a local file.
class DictBackend:
    # backup_filename is a local file where this dict will persist itself.
    def __init__(self, backup_filename):
        self.backup_filename = backup_filename
        try:
            self.kvstore = cPickle.load(open(backup_filename, 'r'))
        except:
            # The file failed to load, so just start with an empty dict.
            self.kvstore = dict()
            pass

    def __del__(self):
        # Persist the dictionary.
        cPickle.dump(self.kvstore, open(self.backup_filename, 'w'))

    def put(self, value):
        # Generate a key by hashing the value.
        key = hashlib.sha512(value).hexdigest()
        self.kvstore[key] = value
        self.incRefCount(key)

    def get(self, key):
        return self.kvstore[key]

    def incRefCount(self, key):
        pass # We don't do garbage collection for now.
        
    def decRefCount(self, key):
        pass # We don't do garbage collection for now.