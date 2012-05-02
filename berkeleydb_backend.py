import bsddb # BerkeleyDB bindings.
import os

# Backend that uses a local BerkeleyDB store. We assume single-threaded use
# of this backend. Since the python bindings only support string values in
# the DB, we encode reference counts as decimal strings prepended to the value
# string and separated by a tilde character.
class BerkeleyDBBackend:
    # filename is a local file where this data will be persisted.
    def __init__(self, filename):
        self.filename = filename
        try:
            self.kvstore = bsddb.hashopen(self.filename)
        except:
            # The file failed to load, so delete it and try again.
            try:
                os.unlink(filename)
            except:
                pass
            self.kvstore = bsddb.hashopen(self.filename)
            
    def __del__(self):
        self.kvstore.close()

    def put(self, key, value):
        try:
            self.incRefCount(key)
        except KeyError:
            # This key does not exist yet, so create it with a reference count of 1.
            self.kvstore[key] = '1~' + value 

    def get(self, key):
        (_, _, value) = self.kvstore[key].partition('~')
        return value

    # Changes the reference count in a record string loaded from the DB.
    def addToRefCount(self, record, delta):            
        # Parse out the reference count, increment it by the delta, and rebuild
        # the record string.
        (refCountStr, _, value) = record.partition('~')
        return str(delta + int(refCountStr)) + '~' + value

    def incRefCount(self, key):
        self.kvstore[key] = self.addToRefCount(self.kvstore[key], 1)
        
    def decRefCount(self, key):
        record = self.addToRefCount(self.kvstore[key], -1)
        
        if record.startswith('0~'):
            # Delete this record from the store.
            del self.kvstore[key]
        else: # Save the updated record.
            self.kvstore[key] = record
            
    def nuke(self):
        self.kvstore.close()
        try:
            os.unlink(self.filename)
        except:
            pass
        self.kvstore = bsddb.hashopen(self.filename)
        
    def flush(self):
        self.kvstore.sync()
        
