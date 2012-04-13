""" 
Type 0 - Directory Blob
     data = {"children": {...}}
Type 1 - Block List Blob
     data = {"blocks": [...]}
Type 2 - Block Blob
     data = {"data": "..."}
"""
from array import array

import cPickle
import hashlib

class Blob(object):
    def __init__(self, key, cntl, parent):
        self.key = key
        self.valid = False
        self.dirty = True
        self.cntl = cntl
        self.parent = parent

    # returns (hash, blob)
    def get_hash_and_blob(self, c):
        cp = cPickle.dumps((c, self.data))
        return (hashlib.sha512(cp).hexdigest(), cp)

    def flush(self):
        if self.dirty:
            cntl.put(self)

    def recursiveFlush(self):
        if self.dirty:
            self.flush()
            self.parent.recursiveFlush()

    @property
    def valid(self):
        return self.valid

    @property
    def invalid(self):
        return not self.valid

    @property
    def dirty(self):
        return self.dirty

    @property
    def clean(self):
        return not self.dirty

"""
This decorate marks a function as one that causes this blob to become dirty. It will take care of marking the blob
as dirty upon entry.
"""
def dirties(fn):
    def wrapped(self, *args, **kwargs):
        self.dirty = True
        return fn(*args, **kwargs)
    return wrapped

"""
This decorator requires the blob to be valid before the function is entered. It takes care of testing for validity,
fetching data if needed, and marking the blob as valid.
"""
def validate(fn):
    def wrapped(self, *args, **kwargs):
        if self.invalid:
            self.cntl.getblob(self.key)
        self.valid = True
        return fn(*args, **kwargs)
    return wrapped

class DirectoryBlob(Blob):
    def __init__(self, key, cntl, parent = None):
        super(DirectoryBlob, self).__init__(self, key, cntl, parent)

    """
    Returns the blob associated with key
    """
    @validate
    def __getitem__(self, item):
        return self.cntl.getblob(self.data[item])

    """
    Sets directory or filename key to point to blob value
    """
    @dirties
    def __setitem__(self, key, value):
        if not isinstance(value, Blob):
            raise TypeError()
        self.data[key] = value

    """
    Deletes child from this directory.
    """
    @dirties
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        # TODO: undirty child so it doesn't flush unnecessarily (??? not sure)
        del self.data[key]

    def __del__(self):
        # TODO: flush if necessary (this will come into play when we start evicting
        # items from cache
        pass

    @property
    def data(self):
        if not hasattr(self, "_data"):
            self._data = dict()
        return self._data

class BlockListBlob(Blob):
    def __init__(self, key, cntl, parent):
        super(DirectoryBlob, self).__init__(self, key, cntl, parent)

    @validate
    def __getitem__(self, item):
        if len(self.blocks) - 1 < item:
            # block list is too short; expand
            self.blocks.extend([None for i in range(item - len(self.blocks) + 1)])
        if self.blocks[item] == None:
            # fetch block
            self.blocks[item] = BlockBlob(self.data[item])
        return self.blocks[item]

    @dirties
    def __setitem__(self, key, value):
        if not isinstance(value, Blob):
            raise TypeError()
        self.blocks[key] = value

    @dirties
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        del self.data[key]
        del self.blocks[key]

    def __del__(self):
        # TODO: flush if necessary
        pass

    @property
    def data(self):
        if not hasattr(self, "_data"):
            self._data = list()
        return self._data

class BlockBlob(Blob):
    def __init__(self, key, cntl, parent):
        super(DirectoryBlob, self).__init__(self, key, cntl, parent)

    @validate
    def __getitem__(self, item):
        if len(self.data) - 1 < item:
            # block list is too short; expand
            self.data.extend([None for i in range(item - len(self.data) + 1)])
        return self.data[item]

    @dirties
    def __setitem__(self, key, value):
        if not isinstance(value, Blob):
            raise TypeError()
        # TODO: add self to parent's dirty list
        self.blocks[key] = value

    @dirties
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        # TODO: add self to parent's dirty list
        del self.data[key]
        del self.blocks[key]

    def __del__(self):
        # TODO: flush if necessary
        pass

    """
    BlockBlob stores data locally as an array of bytes
    """
    @property
    def data(self):
        if not hasattr(self, "_data"):
            self._data = array()
        return self._data