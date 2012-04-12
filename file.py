import blob

PAGE_SIZE = 4096

# Represents a sequential-access file that has been opened.
class File:
    def __init__(self, filename, mode, cntl):
        self.filename = filename
        # TODO actually check this
        self.mode = mode
        self.cntl = cntl

    def create_indirect_block(self, block_list):
        b = blob.Blob.generate_from_type(1)
        b.data["blocks"] = block_list
        return b

    def create_block(self, data):
        b = blob.Blob.generate_from_type(2)
        b.data["data"] = data
        return b

    # in first iteration, we do only full-file writes
    def write(self, data):
        if len(data) > PAGE_SIZE:
            chks = [data[i:i+PAGE_SIZE] for i in range(0, len(data), PAGE_SIZE)]
            blocks = map(self.create_block, chks)
            block_hashes = map(self.cntl.putblob, blocks)
            block = self.create_indirect_block(block_hashes)
        else:
            block = self.create_block(data)
        hash = self.cntl.putblob(block)
        self.cntl.propagate_up_the_tree(self.filename, hash)

    # ...and reads
    def read(self):
        hash = self.cntl.resolve(self.filename)
        block = self.cntl.getblob(hash)
        if block.type == 1: # indirect
            blocks = map(self.cntl.getblob, block.data["blocks"])
            return "".join(map(lambda x: x.data["data"], blocks))
        elif block.type == 2: # direct
            return block.data["data"]
        else:
            raise Exception('unrecognized type!')

    # ... so no seek for the first iteration
    def seek(self, offset, whence):
        raise NotImplementedError

