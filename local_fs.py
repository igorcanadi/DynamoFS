import os
from dynamo_fs import concatPath

# DynamoFS look-alike that just forwards every call to the underlying
# operating system.
class LocalFS:
    # root is the local path to use for this object's root.
    def __init__(self, root):
        self.root = root

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        return LocalFile(filename, mode)

    def isdir(self, path):
        return os.path.isdir(path)

    def rm(self, filename):
        os.unlink(filename)

    def mkdir(self, path, new_dir):
        os.mkdir(concatPath(path, new_dir))

    def ls(self, path):
        os.listdir(path)

    def mv(self, path, old_name, new_name):
        os.rename(concatPath(path, old_name),
                  concatPath(path, new_name))
    
# Object returned by LocalFS.open().
class LocalFile:
    def __init__(self, filename, mode):
        self.f = open(filename, mode)

    def write(self, data):
        self.f.write(data)

    def read(self, max_len):
        return self.f.read(max_len)

    def seek(self, offset, whence):
        self.f.seek(offset, whence)

    def close(self):
        self.f.close()
        del self.f
