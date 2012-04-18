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

    def _file_exists(self, parent, filename):
        return filename in parent.keys()

    def _create_file(self, parent, filename):
        parent[filename] = blob.BlockListBlob(None, self.cntl, parent, True)

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        plist = _get_plist(filename)
        parent = self._find_leaf('/'.join(plist[:-1]))
        if not self._file_exists(parent, filename):
            if mode == 'r':
                raise Exception('File not found')
            else:
                self._create_file(parent, filename)
        return file.File(parent[filename], mode)

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

