import blob
import controller
import cache_manager
import file
import config

# Splits a path into a list of component names.
def _get_plist(path):
    return filter(len, path.split('/'))

def _get_file_and_path(path):
    plist = _get_plist(path)
    return ("/".join(plist[:-1]), plist[-1])

# Gets the leaf name for a path.
def _get_leaf_filename(path):
    return _get_plist(path)[-1]

# Appends a child name to an existing parent path.
def concatPath(parent, child):
    return parent.rstrip('/') + '/' + child.lstrip('/')

class DynamoFS:
    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename, 
                config.CONTROLLER_CACHE_SIZE)
        self.cache_manager = cache_manager.CacheManager(
                config.CACHE_MANAGER_CACHE_SIZE)
        root_hash = self.cntl.get_root_hash()
        try:
            server.get(root_hash)
        except KeyError:
            # if what is in root filename doesn't exist
            # we just create a new filetree
            root_hash = None

        # if root_hash == None, it means we don't have a root, 
        # so this just generates it
        self.root = blob.DirectoryBlob(root_hash, self.cntl, 
                self.cache_manager, None, root_hash == None)

    def cleanup(self):
        self.root.commit()
        self.cache_manager.kill_timer()

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
        parent[filename] = blob.BlockListBlob(None, self.cntl, 
                self.cache_manager, parent, True)

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

    def isdir(self, path):
        parent = self._find_leaf(path)
        return isinstance(parent, blob.DirectoryBlob)

    def rm(self, filename):
        target = self._find_leaf(filename)
        del target.parent[_get_leaf_filename(filename)]

    def mkdir(self, path, new_dir):
        # TODO: add error checking
        # look up parent
        parent = self._find_leaf(path)
        parent[new_dir] = blob.DirectoryBlob(None, self.cntl, 
                self.cache_manager, parent, True)

    def ls(self, path):
        return self._find_leaf(path).keys()

    def mv(self, old_name, new_name):
        (old_path, old_file) = _get_file_and_path(old_name)
        (new_path, new_file) = _get_file_and_path(new_name)
        source = self._find_leaf(old_name)
        target_parent = self._find_leaf(new_path)
        if new_name in target_parent.keys():
            raise Exception('Already exists')
        target_parent[new_file] = source.parent[old_file]
        del source.parent[old_file]

    def get_key_for_sharing(self, path):
        target = self._find_leaf(path)
        target.commit()
        return target.key

    def attach_shared_key(self, path, filename, key):
        target = self._find_leaf(path)
        target[filename] = blob.DirectoryBlob(key, self.cntl, 
                self.cache_manager, target, False)

    def _output_whole_tree(self, node, level):
        if isinstance(node, blob.DirectoryBlob):
            for filename in node.keys():
                print "\t" * level + filename + " (" + node[filename].key[0:5] + ")"
                self._output_whole_tree(node[filename], level + 1)
        elif isinstance(node, blob.BlockListBlob):
            print "\t" * level + "".join([block.data_as_string() for block in node.children]) + \
                    " (" + node.key[0:5] + ")"

    def debug_output_whole_tree(self):
        print "---------------------------- WHOLE TREE -----------------"
#        self.root.commit()
        self._output_whole_tree(self.root, 0)
        print "---------------------------- END -----------------"

    def flush(self):
        # Flush the caches, starting at the top and working down to the
        # lowest level.
        self.cache_manager.flush()
        self.cntl.flush()
        self.cntl.server.flush()
