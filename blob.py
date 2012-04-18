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
    def _get_hash_and_blob(self):
        cp = cPickle.dumps(self.serializable_data())
        hash = hashlib.sha512(cp).hexdigest()
        return (hash, cp)

    def flush(self):
        if self.dirty:
            (self._key, value) = self._get_hash_and_blob()
            self.cntl.putdata(self._key, value)
        self.dirty = False
        if self.parent == None:
            # I'm root
            self.cntl.update_root(self._key)

    def recursiveFlush(self):
        for child in self.children:
            child.recursiveFlush()
        self.flush()

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
        if self.dirty:
            self._key, _ = self._get_hash_and_blob()
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
            self.deserialize_data(self.cntl.getdata(self.key))
        self.valid = True
        self.dirty = False
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
    @validate
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
    @validate
    def __delitem__(self, key):
        """
        Deletes child from this directory.
        """
        # TODO: implement
        raise NotImplementedError()

    def keys(self):
        """
        Returns the filenames of all files in this directory.
        """
        return self.items.keys()

    def getdata(self):
        if not hasattr(self, "_data"):
            self._data = dict()
        return self._data

    def setdata(self, value):
        if not isinstance(value, DirectoryBlob.DATATYPE):
            raise TypeError()
        self._data = value

    data = property(getdata, setdata)

    @property
    def children(self):
        return self.items.values()

class BlockListBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = list

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockListBlob, self).__init__(key, cntl, parent, valid)
        self.blocks = list()

    @validate
    def __getitem__(self, item):
        isBlockValid = False
        if len(self.data) - 1 < item:
            # block list is too short; expand
            isBlockValid = True
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
            self.blocks[item] = BlockBlob(self.data[item], self.cntl, self, isBlockValid)
        return self.blocks[item]

    @dirties
    @validate   # TODO: confirm this is the right order of invocation
    def __setitem__(self, key, value):
        if not isinstance(value, BlockBlob):
            raise TypeError()
        self.blocks[key] = value

    @dirties
    @validate # TODO: confirm this is the right order of invocation
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        del self.blocks[key]

    @property
    def data(self):
        data = list()
        for block in self.blocks:
            data.append(block.key)
        return data

    def deserialize_data(self, data):
        self.blocks = list()
        for key in data:
            self.blocks.append(BlockBlob(key, self.cntl, self))

    @property
    def children(self):
        return self.blocks

class BlockBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = array

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockBlob, self).__init__(key, cntl, parent, valid)

    @validate
    def __getitem__(self, index):
        if len(self.data) - 1 < index:
            raise IndexError("index out of range")
        return self.data[index]

    @dirties
    def __setitem__(self, index, value):
        if len(self.data) - 1 < index:
            # block list is too short; expand
            # TODO: test against PAGE_SIZE
            self.data.extend([0 for i in range(index - len(self.data) + 1)])
        self.data[index] = value

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

    def data_as_string(self):
        return "".join(map(chr, self.data))

    @property
    def children(self):
        return None
