# Represents a sequential-access file that has been opened.
class File:
    filename = ""

    def __init__(self, filename):

        pass

    # in first iteration, we do only full-file writes
    def write(self, data):
        pass

    # ...and reads
    def read(self, data):
        pass

    # ... so no seek for the first iteration
    def seek(self, offset, whence):
        raise NotImplementedError

