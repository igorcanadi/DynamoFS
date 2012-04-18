import blob
import controller
import file

def _get_plist(path):
    return filter(len, path.split('/'))

class DynamoFS:
    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)
        root_hash = self.cntl.get_root_hash()
        # if root_hash == None, it means we don't have a root, so this just generates it
        self.root = blob.DirectoryBlob(root_hash, self.cntl, None, root_hash == None)

    def _find_leaf(self, path):
        # look up parent
        current = self.root
        plist = _get_plist(path)
        for elem in plist:
            current = current[elem]
        return current

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        plist = _get_plist(filename)
        parent = self._find_leaf('/'.join(plist[:-1]))
        return file.File(plist[-1], mode, self.cntl, parent)

    def rm(self, filename):
        target = self._find_leaf(filename)
        del target.parent[target]

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

