import blob
import controller
import file

def _get_plist(path):
    return filter(len, path.split('/'))

def _get_leaf_filename(path):
    return _get_plist(path)[-1]

class DynamoFS:
    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)
        root_hash = self.cntl.get_root_hash()
        # if root_hash == None, it means we don't have a root, so this just generates it
        self.root = blob.DirectoryBlob(root_hash, self.cntl, None, root_hash == None)

    def cleanup(self):
        self.root.recursiveFlush()

    def __del__(self):
        self.cleanup()

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
        leaf_name = plist[-1]
        if not self._file_exists(parent, leaf_name):
            if mode == 'r':
                raise Exception('File not found')
            else:
                self._create_file(parent, leaf_name)
        return file.File(parent[leaf_name], mode)

    def rm(self, filename):
        target = self._find_leaf(filename)
        del target.parent[_get_leaf_filename(filename)]

    def mkdir(self, path, new_dir):
        # TODO: add error checking
        # look up parent
        parent = self._find_leaf(path)
        parent[new_dir] = blob.DirectoryBlob(None, self.cntl, parent, True)

    def ls(self, path):
        return self._find_leaf(path).keys()

    def mv(self, path, old_name, new_name):
        target = self._find_leaf(path + '/' + old_name)
        target.parent[new_name] = target.parent[old_name]
        del target.parent[old_name]

    def get_key_for_sharing(self, path):
        target = self._find_leaf(path)
        target.recursiveFlush()
        return target.key

    def attach_shared_key(self, path, filename, key):
        target = self._find_leaf(path)
        target[filename] = blob.DirectoryBlob(key, self.cntl, target, False)
        print target[filename].data

    def _output_whole_tree(self, b, level):
        if b == None:
            return
        print "%s: %s" % (level, b.key)
        if isinstance(b, blob.DirectoryBlob):
            for i in b.data:
                print level + " " + i
                self._output_whole_tree(b[i], level+"-------")

    def debug_output_whole_tree(self):
        print "---------------------------- WHOLE TREE -----------------"
        self._output_whole_tree(self.root, "")
        print "---------------------------- END -----------------"



