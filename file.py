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

    def _advance_offset(self, size = 1):
        self._offset[1] += size 
        if self._offset[1] >= PAGE_SIZE:
            self._offset[0] += 1
            self._offset[1] -= PAGE_SIZE

    def _offset_at_the_end(self):
        if len(self.blob.children) <= self._offset[0]:
            return True
        if self.blob[self._offset[0]].size() <= self._offset[1]:
            return True
        return False

    def write_array(self, data):
        if self.mode != 'w':
            raise Exception('Not writable, sorry')
        data_index = 0
        while data_index < len(data):
            free_space_in_page = PAGE_SIZE - self._offset[1]
            self.blob[self._offset[0]].write(self._offset[1],
                    data[data_index:(data_index + free_space_in_page)])
            self._advance_offset(min(free_space_in_page, len(data) - data_index))
            data_index += free_space_in_page

    def write(self, data):
        self.write_array(map(ord, data))

    def read_array(self, max_len):
        if self.mode != 'r':
            raise Exception('Not readable, sorry')
        ret = array('B')
        while len(ret) < max_len:
            if self._offset_at_the_end():
                break
            reading_bytes = self.blob[self._offset[0]].size() - \
                self._offset[1]
            reading_bytes = min(reading_bytes, max_len - len(ret))
            ret.extend(self.blob[self._offset[0]].read(self._offset[1], reading_bytes))
            self._advance_offset(reading_bytes)
        return ret

    def read(self, max_len):
        return "".join(map(chr, self.read_array(max_len)))

    # The lseek() function allows the file offset to be set beyond the end of
    # the existing end-of-file of the file. If data is later written at this
    # point, subsequent reads of the data in the gap return bytes of zeros
    # (until data is actually written into the gap).
    def seek(self, offset, whence):
        if whence == SEEK_SET:
            self._offset = [offset / PAGE_SIZE, offset % PAGE_SIZE]
        elif whence == SEEK_CUR:
            self.seek(self._offset[0] * PAGE_SIZE + self._offset[1] + offset, SEEK_SET)
        elif whence == SEEK_END:
            last_block = len(self.blob.children) - 1
            if last_block == -1:
                file_len = 0
            else:
                file_len = last_block * PAGE_SIZE + self.blob[last_block].size()
            self.seek(file_len + offset, SEEK_SET)

    def close(self):
        self.blob.commit()
