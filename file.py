import blob
from array import array

PAGE_SIZE = 4096

# Represents a sequential-access file that has been opened.
class File:
    def __init__(self, filename, mode, cntl, parent):
        self.filename = filename
        # TODO actually check this
        self.mode = mode
        self.cntl = cntl
        self.parent = parent

    # in first iteration, we do only full-file writes
    def write(self, data):
        data = array('B', data)
        self.parent[self.filename] = \
            blob.BlockListBlob(None, self.cntl, self.parent, True)
        chks = [data[i:i+PAGE_SIZE] for i in range(0, len(data), PAGE_SIZE)]
        for i in range(len(chks)):
            self.parent[self.filename][i].setdata(chks[i])

    # ...and reads
    def read(self):
        blocklist = self.parent[self.filename]
        return "".join(map(lambda x: x.getdata(), blocklist.getdata()))

    # ... so no seek for the first iteration
    def seek(self, offset, whence):
        raise NotImplementedError

    def close(self):
        self.parent[self.filename].flush()

