import blob
import controller
import file

class DynamoFS:
    root_filename = ""
    root_hash = ""
    server = False

    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)

    def _find_leaf(self, path):
        # look up parent
        current = self.root
        plist = filter(len, path.split('/'))
        for elem in plist:
            current = current[elem]
        return current

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        return file.File(filename, mode, self.cntl)

    def rm(self, filename):
        self.cntl.rm(filename)

    def mkdir(self, path, new_dir):
        # TODO: add error checking
        # look up parent
        parent = self._find_leaf(path)
        parent[new_dir] = blob.DirectoryBlob(None, self.cntl, parent, True)

    def ls(self, path):
        return self._find_leaf(path).keys()

    # Renames a file or directory.
    def rename(self, old_name, new_name):
        pass

