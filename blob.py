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
            if not self.parent == None:
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


def dirties(fn):
    """
    This decorate marks a function as one that causes this blob to become dirty. It will take care of marking the blob
    as dirty upon entry.
    """
    def wrapped(self, *args, **kwargs):
        self.dirty = True
        return fn(*args, **kwargs)
    return wrapped


def validate(fn):
    """
    This decorator requires the blob to be valid before the function is entered. It takes care of testing for validity,
    fetching data if needed, and marking the blob as valid.
    """
    def wrapped(self, *args, **kwargs):
        if self.invalid:
            self.cntl.getblob(self.key)
        self.valid = True
        return fn(*args, **kwargs)
    return wrapped

class DirectoryBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = dict

    def __init__(self, key, cntl, parent = None, valid = False):
        super(DirectoryBlob, self).__init__(self, key, cntl, parent, valid)

    @validate
    def __getitem__(self, item):
        """
        Returns the blob associated with key
        """
        return self.cntl.getblob(self.data[item])

    @dirties
    def __setitem__(self, key, value):
        """
        Sets directory or filename key to point to blob value
        """
        if not isinstance(value, Blob):
            raise TypeError()
        self.data[key] = value

    @dirties
    def __delitem__(self, key):
        """
        Deletes child from this directory.
        """
        # TODO: decrement reference count of deleted object
        # TODO: undirty child so it doesn't flush unnecessarily (??? not sure)
        del self.data[key]

    def __del__(self):
        # TODO: flush if necessary (this will come into play when we start evicting
        # items from cache)
        pass

    @property
    def data(self):
        if not hasattr(self, "_data"):
            self._data = dict()
        return self._data

    @property.getter
    def data(self, value):
        if not isinstance(value, DirectoryBlob.DATATYPE):
            raise TypeError()
        self._data = value

class BlockListBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = list

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockListBlob, self).__init__(self, key, cntl, parent, valid)

    @validate
    def __getitem__(self, item):
        if len(self.data) - 1 < item:
            # block list is too short; expand
            self.blocks.extend([
                BlockBlob(None, cntl, self, True)
                    for i in range(item - len(self.blocks) + 1)
            ])
            self.data.extend([
                None
                    for i in range(item - len(self.blocks) + 1)
            ])
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

    @property.getter
    def data(self, value):
        if not isinstance(value, BlockListBlob.DATATYPE):
            raise TypeError()
        self._data = value

class BlockBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = array

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockBlob, self).__init__(self, key, cntl, parent, valid)

    @validate
    def __getitem__(self, item):
        if len(self.data) - 1 < item:
            # block list is too short; expand
            self.data.extend([0 for i in range(item - len(self.data) + 1)])
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

    @property
    def data(self):
        """
        BlockBlob stores data locally as an array of bytes
        """
        if not hasattr(self, "_data"):
            self._data = array('B')
        return self._data

    @property.getter
    def data(self, value):
        if not isinstance(value, BlockBlob.DATATYPE):
            raise TypeError()
        self._data = value