import blob
from array import array

PAGE_SIZE = 4096

# Represents a sequential-access file that has been opened.
class File:
    def __init__(self, blob, mode):
        self.blob = blob
        self.mode = mode

    # in first iteration, we do only full-file writes
    def write(self, data):
        if self.mode != 'w':
            raise Exception('not writtable, sorry')
        data = array('B', data)
        chks = [data[i:i+PAGE_SIZE] for i in range(0, len(data), PAGE_SIZE)]
        for i in range(len(chks)):
            self.blob[i].setdata(chks[i])

    # ...and reads
    def read(self):
        if self.mode != 'r':
            raise Exception('not readable, sorry')
        return "".join(map(lambda x: str(x.data_as_string()), self.blob.blocks))

    # ... so no seek for the first iteration
    def seek(self, offset, whence):
        raise NotImplementedError

    def close(self):
        self.blob.flush()
