""" 
GenericBlob will be a tuple of (type, blob) which will
represent every blob we store on the server.
Type -1 - Uninitialized
Type 0 - Directory Blob
Type 1 - Block List Blob
Type 2 - Block Blob
""" 

import directory_blob
import block_list_blob 
import block_blob 
import cPickle
import hashlib

class GenericBlob:
    type = -1
    blob = ""

    def construct_from_object(self, object):
        if isinstance(object, directory_blob.DirectoryBlob):
            self.type = 0
        elif isinstance(object, block_list_blob.BlockListBlob):
            self.type = 1
        elif isinstance(object, block_blob.BlockBlob):
            self.type = 2
        else:
            raise Exception("unknown class")
        self.blob = cPickle.dumps(object)

    # returns (hash, blob)
    def get_hash_and_blob(self):
        cp = cPickle.dumps(self)
        return (hashlib.sha512(cp).hexdigest(), cp)

