import blob
import controller
import file

class DynamoFS:
    root_filename = ""
    root_hash = ""
    server = False

    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)

    def _get_plist(path):
        return filter(len, path.split('/'))

    def _find_leaf(self, path):
        # look up parent
        current = self.cntl.root
        plist = DynamoFS._get_plist(path)
        for elem in plist:
            current = current[elem]
        return current

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        plist = DynamoFS._get_plist(filename)
        parent = self._find_leaf('/'.join(plist[:-1]))
        # TODO: finish

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

