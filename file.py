import blob
from array import array

PAGE_SIZE = 4096
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

# Represents a sequential-access file that has been opened.
class File:
    def __init__(self, blob, mode):
        self.blob = blob
        self.mode = mode
        self._offset = [0, 0]

    def _advance_offset(self):
        self._offset[1] += 1
        if self._offset[1] == PAGE_SIZE:
            self._offset[0] += 1
            self._offset[1] = 0

    def _offset_at_the_end(self):
        if len(self.blob.children) <= self._offset[0]:
            return True
        if self.blob[self._offset[0]].size() <= self._offset[1]:
            return True
        return False

    def write(self, data):
        if self.mode != 'w':
            raise Exception('not writtable, sorry')

        for d in data:
            self.blob[self._offset[0]][self._offset[1]] = ord(d)
            self._advance_offset()

    def read(self, max_len):
        if self.mode != 'r':
            raise Exception('not readable, sorry')
        ret = ""
        for i in range(max_len):
            if self._offset_at_the_end():
                break
            ret += chr(self.blob[self._offset[0]][self._offset[1]])
            self._advance_offset()
        return ret

    # The lseek() function allows the file offset to be set beyond the end of
    # the existing end-of-file of the file. If data is later written at this
    # point, subsequent reads of the data in the gap return bytes of zeros
    # (until data is actually written into the gap).
    def seek(self, offset, whence):
        if whence == SEEK_SET:
            self._offset = (offset / PAGE_SIZE, offset % PAGE_SIZE)
        elif whence == SEEK_CUR:
            self.seek(self._offset[0] * PAGE_SIZE + self._offset[1] + offset, SEEK_SET)
        elif whence == SEEK_END:
            last_block = len(self.blob.children) - 1
            file_len = last_block * PAGE_SIZE + self.blob[last_block].size()
            self.seek(file_len + offset)

    def close(self):
        self.blob.flush()
