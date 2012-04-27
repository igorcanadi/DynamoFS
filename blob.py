from array import array

import cPickle
import hashlib

def dirties(fn):
    """
    This decorator marks a function as one that causes this blob to become dirty. It will take care of marking the blob
    as dirty upon entry.
    """
    def wrapped(self, *args, **kwargs):
        self._assert_valid_state()
        # I have been accessed, don't evict me from cache!
        self.cache_manager.add_to_cache(self)
        if self.clean:
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
        self._assert_valid_state()
        if self.invalid:
            # I have been accessed, don't evict me from cache!
            self.cache_manager.add_to_cache(self)
            self._blob = self.cntl.getdata(self._key)
            self._deserialize_data(self._blob)
            self.dirty = False
            self.valid = True
            self._key = None
        return fn(self, *args, **kwargs)
    return wrapped

class Blob(object):
    def __init__(self, key, cntl, cache_manager, parent, valid = False):
        """
        key: the key of the data that this blob represents.
        cntl: the controller object this blob should use to access the backing store.
        parent: the parent blob of this blob.
        valid: whether this blob is valid -- that is, should the data be fetched from
            the backend before this blob can be read?
        """
        self._key = key
        self._blob = None
        self.parent = parent
        self.valid = valid
        self.dirty = valid
        self.cntl = cntl
        self.cache_manager = cache_manager
        self._assert_valid_state()

    def my_name(self):
        if self.parent == None:
            return "/"
        else:
            return self.parent.my_name() + "/" + self.parent.locate(self)

    def _assert_valid_state(self):
        assert(self._key != None or self.valid == True)
        # TODO: is this valid assert?
#        assert(self._key != None or self.dirty == True)
        assert(self.valid == True or self.dirty == False)

    def _delete_data(self):
        """
        You're being removed from cache, clear your data
        """
        raise NotImplementedError()

    def evict(self):
        """ 
        This is called by a cache manager to let me know that I need to 
        evict my data field from main memory. It only does anything if this blob
        is both dirty and valid.
        """
        self._assert_valid_state()
        if self.valid:
            self.commit()
            for child in self.children:
                child.evict()
        self.dirty = False
        self.valid = False
        self._delete_data()
        self._blob = None
        self.cache_manager.remove_from_cache(self)
        self._assert_valid_state()

    def commit(self):
        """
        Calls _flush_down on all children, then calls _flush_up on self. When commit
        returns to an external caller, the file system will be in a consistent and
        up-to-date state. If this node is clean, commit is a no-op.
        """
        self._assert_valid_state()
        if self.dirty or self._key == None:
            self._flush_down()
        self._flush_up()

    def _flush_up(self):
        """
        If self.dirty, then call _flush on self. If parent exists, call _flush_up
        on parent. If no parent, update root on controller with self's key.
        """
        if self.parent != None:
            self.parent._flush_up()
        else:
            self.cntl.update_root(self.key)

    def _flush_down(self):
        """
        For each child, call _flush_down on that child. Then, call _flush on self.
        """
        if self.dirty or self._key == None:
            for child in self.children:
                child._flush_down()
            self._flush()

    def _flush(self):
        """
        If this blob is dirty, push this blob to store and set this blob to clean.
        """
        if self.dirty or self._key == None:
            self.cntl.putdata(self.key, self.blob)
            self.dirty = False

    @property
    def invalid(self):
        """
        Convenience property. Returns not self.valid.
        """
        return not self.valid

    @property
    def clean(self):
        """
        Convenience property. Returns not self.dirty.
        """
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

    @validate
    def _update_hash_and_blob(self):
        """
        After this method is called, self._blob will contain the serialized representation
        of this blob's data, and self._key will contain the hash of self._blob.
        """
        self._blob = self._serialize_data()
        cp = self._blob
        self._key = hashlib.sha512(cp).hexdigest()

    @property
    def key(self):
        """
        Returns the hash of this blob -- that is, the key under which this blob should
        be stored in the backend. The returned key is guaranteed to be up-to-date according
        to the data currently stored. Does not check for whether this blob is valid.
        """
        if self._key == None:
            self._update_hash_and_blob()
        return self._key

    @property
    def blob(self):
        """
        Returns the serialized data associated with this blob. This is the data that should
        be stored in the backend. The serialized data is based on the current locally stored
        data -- this method does not check for whether the blob is valid.
        """
        if self._blob == None:
            self._update_hash_and_blob()
        return self._blob

    def getdirty(self):
        """
        Getter for dirty property -- tells you whether the blob has uncommitted changes.
        """
        return self._dirty

    def setdirty(self, value):
        """
        Setter for dirty property -- tells you whether the blob has uncommitted changes.
        """
        
        if value == True and self.parent != None and self.parent.dirty == False:
            self.parent.dirty = True
        self._dirty = value

    dirty = property(getdirty, setdirty)

    @property
    def children(self):
        """
        Each subclass should implement this method to return a list of Blob objects, each
        of which is a child of the current object. DirectoryBlobs should return a list of
        subdirectories/files; BlockListBlobs should return a list of BlockBlobs; BlockBlobs
        should return an empty list.
        """
        raise NotImplementedError()

    def __hash__(self):
        return id(self)

class DirectoryBlob(Blob):
    """
    Represents a directory. Can be used like a dict. Example usage:
    >>> dirblob = DirectoryBlob(None, cntl, cache_manager, None, True)
    >>> dirblob['usr'] = DirectoryBlob(None, cntl, cache_manager, dirblob, True)
    >>> dirblob['usr']['bin'] = DirectoryBlob(None, cntl, cache_manager, dirblob['usr'], True)
    >>> dirblob['usr']['bin']['README'] = BlockListBlob(none, cntl, cache_manager, dirblob['usr']['bin'], True)
    The file system now looks like this:
    usr
        bin
            README
    where usr and bin are directories and README is a file.

    Delete items like so:
    >>> del dirblob['usr']['bin']['README']

    To flush everything, including the children, do:
    >>> dirblob.recursiveFlush()
    """
    def __init__(self, key, cntl, cache_manager, parent = None, valid = False):
        super(DirectoryBlob, self).__init__(key, cntl, cache_manager, parent, valid)
        if valid:
            self.items = dict()

    def _delete_data(self):
        self.items = dict()

    def locate(self, other):
        for k, v in self.items.items():
            if v == other:
                return k
        raise Exception("%s not in %s" % (other, self.items))

    @validate
    def __getitem__(self, filename):
        """
        Returns the blob associated with key
        """
        if filename not in self.items:
            raise IOError("%s not found" % filename)
        return self.items[filename]

    @validate
    @dirties
    def __setitem__(self, filename, blob):
        """
        Sets directory or filename key to point to blob value
        """
        
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
            self.items[filename] = itemclass(item, self.cntl, 
                    self.cache_manager, self)

class BlockListBlob(Blob):
    """
    Represents a block list. Can be accessed like a list. For example:
    >>> blocks = BlockListBlob(None, cntl, parent, True)
    >>> blocks[0][0] = "H"
    >>> print chr(blocks[0][0])
    H
    >>> blocks[0].extend(array("B", "ello, world!"))
    >>> print map(chr, blocks[0])
    Hello, world!
    Each element in this object is a BlockBlob.
    """
    def __init__(self, key, cntl, cache_manager, parent, valid = False):
        super(BlockListBlob, self).__init__(key, cntl, cache_manager, parent, valid)
        if valid:
            self.blocks = list()

    def _delete_data(self):
        self.blocks = list()

    def locate(self, other):
        return "Block of " + self.parent.locate(self)

    @validate
    def __getitem__(self, item):
        # I have been accessed, don't evict me from cache!
        self.cache_manager.add_to_cache(self)
        if len(self.blocks) - 1 < item:
            # block list is too short; expand
            self.dirty = True
            self.blocks.extend([
                BlockBlob(None, self.cntl, self.cache_manager, self, True)
                    for i in range(item - len(self.blocks) + 1)
            ])
        return self.blocks[item]

    @validate
    @dirties
    def __setitem__(self, key, value):
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
            self.blocks.append(BlockBlob(key, self.cntl, self.cache_manager, self))

    @property
    @validate
    def children(self):
        return self.blocks

class BlockBlob(Blob):
    """
    Represents a block of data. Can be treated like an array. The underlying representation
    used is array.array. Example usage:
    >>> block = BlockBlob(None, cntl, cache_manager, parent, True)
    >>> print block.data_as_string()

    >>> block.extend(array("B", "Good bye."))
    >>> print block.data_as_string()
    Good bye.
    """
    def __init__(self, key, cntl, cache_manager, parent, valid = False):
        super(BlockBlob, self).__init__(key, cntl, cache_manager, parent, valid)
        if self.valid:
            self.data = array('B')

    def _delete_data(self):
        self.data = array('B')

    @validate
    def __getitem__(self, index):
        # I have been accessed, don't evict me from cache!
        self.cache_manager.add_to_cache(self)
        if len(self.data) - 1 < index:
            raise IndexError("index out of range")
        return self.data[index]

    @validate
    @dirties
    def __setitem__(self, index, value):
        if len(self.data) - 1 < index:
            # block list is too short; expand
            self.data.extend([0 for i in range(index - len(self.data) + 1)])
        self.data[index] = value

    @validate
    @dirties
    def write(self, index, value):
        """
        Batch write operation. Writes value to data starting at index. Overwrites
        existing data.
        """
        if len(self.data) < index + len(value):
            self.data.extend([0 for i in range(index + len(value) - len(self.data))])
        self.data[index:(index + len(value))] = array("B", value)

    @validate
    def read(self, index, size):
        """
        Returns sub array [index, index+size)
        """
        if len(self.data) - 1 < index + size - 1:
            raise IndexError("index out of range")
        return self.data[index:index + size]

    @validate
    @dirties
    def append(self, value):
        """
        Appends value to this block's data. Value must be an integer.
        """
        # TODO: test against PAGE_SIZE
        self.data.append(value)

    @validate
    @dirties
    def extend(self, values):
        """
        Appends each of a list of values to this block's data. The values must
        be integers.
        """
        # TODO: test against PAGE_SIZE
        self.data.extend(values)

    def _serialize_data(self):
        return cPickle.dumps(self.data.tostring())

    def _deserialize_data(self, data):
        self.data = array("B")
        self.data.fromstring(cPickle.loads(data))

    @validate
    def data_as_string(self):
        """
        Returns the data in this block as a string
        """
        return "".join(map(chr, self.data))

    @validate
    def size(self):
        return len(self.data)

    @property
    def children(self):
        """
        Block's don't have children, so return an empty list.
        """
        return list()
