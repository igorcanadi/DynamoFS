import blob

class Controller:
    def __init__(self, server, root_filename):
        self.server = server
        self.root_filename = root_filename
        try:
            self.root_hash = open(root_filename, 'r').read().split('\n')[0]
        except IOError, e:
            pass

    def getdata(self, hash):
        return self.server.get(hash)

    # returns hash
    def putdata(self, blob):
        (hash, value) = blob.get_hash_and_blob(blob.__class__)
        self.server.put(hash, value)
        return hash

    def update_root(self, new_root):
        self.root_hash = new_root
        open(self.root_filename, 'w').write(new_root)

    # get me all keys from ancestors 
    def get_all_parents(self, filename):
        hash = self.root_hash
        ret = [hash]
        path = filter(len, filename.split('/'))
        for p in path:
            path_blob = self.getdata(hash)
            if (path_blob.type != 0) or (p not in path_blob.data["children"]):
                raise Exception('File not found')
            hash = path_blob.data["children"][p]
            ret.append(hash)
        return ret

    # give me back the key of file or directory
    def resolve(self, filename):
        return self.get_all_parents(filename)[-1]

    def propagate_up_the_tree(self, path, hash):
        plist = filter(len, path.split('/'))
        phash = self.get_all_parents("/" + "/".join(plist[:-1]))

        for i in range(len(plist)-1, -1, -1):
            b = self.getdata(phash[i])
            if b.type != 0:
                raise Exception('File not found')
            b.data["children"][plist[i]] = hash
            (hash, value) = b.get_hash_and_blob()
            self.server.put(hash, value)

        self.update_root(hash)

    def mkdir(self, path, new_dir):
        hash = self.putdata(blob.Blob.generate_from_type(0))
        self.propagate_up_the_tree(path + '/' + new_dir, hash)

    def rm(self, path):
        plist = filter(len, path.split('/'))
        parent = "/".join(plist[0:len(plist)-1])
        parent_hash = self.resolve(parent)
        b = self.getdata(parent_hash)
        del b.data["children"][plist[len(plist)-1]]
        self.propagate_up_the_tree(parent, self.putdata(b))

    def ls(self, path):
        hash = self.resolve(path)
        path_blob = self.getdata(hash)
        return path_blob.data["children"].keys()

