import os
from dynamo_fs import concatPath

# DynamoFS look-alike that just forwards every call to the underlying
# operating system.
class LocalFS:
    # root is the local path to use for this object's root.
    def __init__(self, root):
        os.mkdir(root)
        self.root = root
        
    def _fromRoot(self, path):
        return concatPath(self.root, path)

    # mode can be 'r' or 'w'
    def open(self, path, mode):
        path = self._fromRoot(path)
        return LocalFile(path, mode)

    def isdir(self, path):
        path = self._fromRoot(path)
        return os.path.isdir(path)

    def rm(self, path):
        path = self._fromRoot(path)
        if os.path.isdir(path):
            os.rmdir(path)
        else:
            os.unlink(path)

    def mkdir(self, path, newName):
        path = self._fromRoot(path)
        os.mkdir(concatPath(path, newName))

    def ls(self, path):
        path = self._fromRoot(path)
        return os.listdir(path)

    def mv(self, old_name, new_name):
        old_name = self._fromRoot(old_name)
        new_name = self._fromRoot(new_name)
        os.rename(old_name, new_name)
    
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
