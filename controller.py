
class Controller:
    def __init__(self, server, root_filename):
        self.server = server
        self.root_filename = root_filename
        try:
            self.root_hash = open(root_filename, 'r').read().split('\n')[0]
        except IOError:
            self.root_hash = None

    def getdata(self, hash):
        return self.server.get(hash)

    def putdata(self, hash, value):
        self.server.put(hash, value)

    # return None if root hash not present
    def get_root_hash(self):
        return self.root_hash

    def update_root(self, new_root_hash):
        open(self.root_filename, 'w').write(new_root_hash)
