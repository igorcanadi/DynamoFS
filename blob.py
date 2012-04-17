from array import array

import cPickle
import hashlib

class Blob(object):
    def __init__(self, key, cntl, parent, valid = False):
        self._key = key
        self.valid = valid
        self.dirty = True
        self.cntl = cntl
        self.parent = parent

    # returns (hash, blob)
    def get_hash_and_blob(self, c):
        cp = cPickle.dumps((c, self.serializable_data()))
        self._key = hashlib.sha512(cp).hexdigest()
        return (self._key, cp)

    def flushSelfOnly(self):
        if self.dirty:
            self.key = self.cntl.putdata(self)
        self.dirty = False

    def flush(self):
        if self.dirty:
            self._key = self.cntl.putdata(self)
            if not self.parent == None:
                self.parent.flush()
        self.dirty = False

    @property
    def invalid(self):
        return not self.valid

    @property
    def clean(self):
        return not self.dirty

    def serializable_data(self):
        """
        Override this method if the data format can't be serialized by cPickle normally.
        """
        return self.data

    def deserialize_data(self, data):
        """
        Override this method if the data attribute cannot be serialized by cPickle normally
        """
        self._data = data.fromstring()

    @property
    def key(self):
        self._key, _ = self.get_hash_and_blob(self.__class__)
        return self._key

def dirties(fn):
    """
    This decorator marks a function as one that causes this blob to become dirty. It will take care of marking the blob
    as dirty upon entry.
    """
    def wrapped(self, *args, **kwargs):
        self.dirty = True
        return fn(self, *args, **kwargs)
    return wrapped

def validate(fn):
    """
    This decorator requires the blob to be valid before the function is entered. It takes care of testing for validity,
    fetching data if needed, and marking the blob as valid.
    """
    def wrapped(self, *args, **kwargs):
        if self.invalid:
            self.deserialize_data(self.cntl.getdata(self.key)[1])
        self.valid = True
        return fn(self, *args, **kwargs)
    return wrapped

class DirectoryBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = dict

    def __init__(self, key, cntl, parent = None, valid = False):
        super(DirectoryBlob, self).__init__(key, cntl, parent, valid)
        self.items = dict()

    @validate
    def __getitem__(self, filename):
        """
        Returns the blob associated with key
        """
        if not isinstance(filename, str):
            raise TypeError("item was of type %s, expected %s" % (type(filename), str))
        # TODO (jim) if it's not there, raise an error, don't create
        if filename not in self.items:
            itemclass, itemHash = self.data[filename]
            self.items[filename] = itemclass(itemHash, self.cntl, self)
        return self.items[filename]

    @dirties
    def __setitem__(self, filename, blob):
        """
        Sets directory or filename key to point to blob value
        """
        if not isinstance(filename, str):
            raise TypeError("key was of type %s, expected %s" % (type(filename), str))
        if not isinstance(blob, DirectoryBlob) and not isinstance(blob, BlockListBlob):
            raise TypeError("key was of type %s, expected %s or %s" % (type(filename), DirectoryBlob, BlockListBlob))
        self.items[filename] = blob
        self.data[filename] = (blob.__class__, blob.key)

    @dirties
    def __delitem__(self, key):
        """
        Deletes child from this directory.
        """
        # TODO: implement
        raise NotImplementedError()

    def __del__(self):
        # TODO: flush if necessary (this will come into play when we start evicting
        # items from cache)
        pass

    def keys(self):
        return self.data.keys()

    def getdata(self):
        if not hasattr(self, "_data"):
            self._data = dict()
        return self._data

    def setdata(self, value):
        if not isinstance(value, DirectoryBlob.DATATYPE):
            raise TypeError()
        self._data = value

    data = property(getdata, setdata)

class BlockListBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = list

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockListBlob, self).__init__(key, cntl, parent, valid)
        self.blocks = list()

    @validate
    def __getitem__(self, item):
        if len(self.data) - 1 < item:
            # block list is too short; expand
            self.blocks.extend([
                BlockBlob(None, self.cntl, self, True)
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

    def getdata(self):
        if not hasattr(self, "_data"):
            self._data = list()
        return self._data

    def setdata(self, value):
        if not isinstance(value, BlockListBlob.DATATYPE):
            raise TypeError()
        self._data = value

    data = property(getdata, setdata)

class BlockBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = array

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockBlob, self).__init__(key, cntl, parent, valid)

    @validate
    def __getitem__(self, item):
        if len(self.data) - 1 < item:
            # TODO: should this actually raise an IndexOutOfBounds exception?
            # block list is too short; expand
            self.data.extend([0 for i in range(item - len(self.data) + 1)])
        return self.data[item]

    @dirties
    def __setitem__(self, key, value):
        if not isinstance(value, Blob):
            raise TypeError()
        # TODO: add self to parent's dirty list
        self.data[key] = value

    @dirties
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        # TODO: add self to parent's dirty list
        del self.data[key]

    def __del__(self):
        # TODO: flush if necessary
        pass

    def getdata(self):
        """
        BlockBlob stores data locally as an array of bytes
        """
        if not hasattr(self, "_data"):
            self._data = array('B')
        return self._data

    def setdata(self, value):
        if not isinstance(value, BlockBlob.DATATYPE):
            raise TypeError()
        self._data = value

    data = property(getdata, setdata)

    def serializable_data(self):
        return self.data.tostring()

    def deserialize_data(self, data):
        self.data = data.fromstring()
