from array import array

import cPickle
import hashlib

def dirties(fn):
    """
    This decorator marks a function as one that causes this blob to become dirty. It will take care of marking the blob
    as dirty upon entry.
    """
    def wrapped(self, *args, **kwargs):
        self.dirty = True
        self._key = None
        self._blob = None
        return fn(self, *args, **kwargs)
    return wrapped

def validate(fn):
    """
    This decorator requires the blob to be valid before the function is entered. It takes care of testing for validity,
    fetching data if needed, and marking the blob as valid.
    """
    def wrapped(self, *args, **kwargs):
        if self.invalid:
            self._deserialize_data(self.cntl.getdata(self._key))
            self.dirty = False
        self.valid = True
        self._blob = None
        return fn(self, *args, **kwargs)
    return wrapped

class Blob(object):
    def __init__(self, key, cntl, parent, valid = False):
        """
        _key is None if and only if valid is True.
        """
        if (key == None) != valid:
            raise
        self._key = key
        self._blob = None
        self.parent = parent
        self.valid = valid
        self.dirty = True
        self.cntl = cntl

    def flush(self):
        """
        If this Blob is dirty, flush it to the backing store using the controller's
        putdata method. Also, if this Blob has a parent, call parent.flush(). If
        this Blob does not have a parent, update the root on the controller.
        """
        if self.dirty:
            self.cntl.putdata(self.key, self.blob)
            # If i'm root, update root
            if self.parent == None:
                self.cntl.update_root(self.key)
            else:
                self.parent.flush()
            self.dirty = False

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

    def _serialize_data(self):
        """
        This method returns the blob's data serialized by cPickle.dumps. Data may be,
        for example, a dict mapping filenames to keys (for a directory), or a list of
        keys (for a block list), or a string representing a list of bytes (for a block).
        """
        raise NotImplementedError()

    def _deserialize_data(self, data):
        """
        The parameter to this function is an object that was previously output by a call
        to serializable_data on a blob of this blob's type. It will use the data to
        initialize this blob.
        """
        raise NotImplementedError()

    def _update_hash_and_blob(self):
#        if self.dirty and self.valid:
#        if self._blob == None:
            self._blob = self._serialize_data()
#        if self._key == None:
            cp = self._blob
            self._key = hashlib.sha512(cp).hexdigest()

    @property
    def key(self):
        self._update_hash_and_blob()
        return self._key

    @property
    def blob(self):
        self._update_hash_and_blob()
        return self._blob

    def getdirty(self):
        return self._dirty

    def setdirty(self, value):
        if not isinstance(value, bool):
            raise TypeError("dirty variable can be only True or False")
        if value == True and self.parent != None:
            self.parent.dirty = True
        self._dirty = value

    dirty = property(getdirty, setdirty)

class DirectoryBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = dict

    def __init__(self, key, cntl, parent = None, valid = False):
        super(DirectoryBlob, self).__init__(key, cntl, parent, valid)
        if valid:
            self.items = dict()

    @validate
    def __getitem__(self, filename):
        """
        Returns the blob associated with key
        """
        if not isinstance(filename, str):
            raise TypeError("item was of type %s, expected %s" % (type(filename), str))
        if filename not in self.items:
            raise IOError()
        return self.items[filename]

    @validate
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

    @validate
    @dirties
    def __delitem__(self, key):
        """
        Deletes child from this directory.
        """
        # TODO: add garbage collection and everything 
        del self.items[key]

    @validate
    def keys(self):
        """
        Returns the filenames of all files in this directory.
        """
        return self.items.keys()

    @property
    @validate
    def children(self):
        return self.items.values()

    def _serialize_data(self):
        data = dict()
        for filename, blob in self.items.items():
            data[filename] = (blob.__class__, blob.key)
        return cPickle.dumps(data)

    def _deserialize_data(self, data):
        self.items = dict()
        for filename, (itemclass, item) in cPickle.loads(data).items():
            self.items[filename] = itemclass(item, self.cntl, self)

class BlockListBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = list

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockListBlob, self).__init__(key, cntl, parent, valid)
        if valid:
            self.blocks = list()

    @validate
    def __getitem__(self, item):
        if len(self.blocks) - 1 < item:
            # block list is too short; expand
            self.dirty = True
            self.blocks.extend([
                BlockBlob(None, self.cntl, self, True)
                    for i in range(item - len(self.blocks) + 1)
            ])
        return self.blocks[item]

    @validate
    @dirties
    def __setitem__(self, key, value):
        if not isinstance(value, BlockBlob):
            raise TypeError()
        self.blocks[key] = value

    @validate
    @dirties
    def __delitem__(self, key):
        # TODO: decrement reference count of deleted object
        del self.blocks[key]

    def _serialize_data(self):
        """
        Returns a list of the keys of the blocks in this block list.
        """
        data = list()
        for block in self.blocks:
            data.append(block.key)
        return cPickle.dumps(data)

    def _deserialize_data(self, data):
        """
        Expects a list of keys of blocks in this block list; initializes the
        block list with invalid BlockBlob objects.
        """
        self.blocks = list()
        for key in cPickle.loads(data):
            self.blocks.append(BlockBlob(key, self.cntl, self))

    @property
    def children(self):
        return self.blocks

class BlockBlob(Blob):
    # DATATYPE is the type that the data is stored in for this class
    DATATYPE = array

    def __init__(self, key, cntl, parent, valid = False):
        super(BlockBlob, self).__init__(key, cntl, parent, valid)
        if self.valid:
            self.data = array('B')

    @validate
    def __getitem__(self, index):
        if len(self.data) - 1 < index:
            raise IndexError("index out of range")
        return self.data[index]

    @validate
    @dirties
    def __setitem__(self, index, value):
        if len(self.data) - 1 < index:
            # block list is too short; expand
            # TODO: test against PAGE_SIZE
            self.data.extend([0 for i in range(index - len(self.data) + 1)])
        self.data[index] = value

    def _serialize_data(self):
        return cPickle.dumps(self.data.tostring())

    def _deserialize_data(self, data):
        self.data = cPickle.loads(array("B").fromstring(data))

    def data_as_string(self):
        """
        Returns the data in this block as a string
        """
        return "".join(map(chr, self.data))

    @property
    def children(self):
        """
        Block's don't have children, so return an empty list.
        """
        return list()
