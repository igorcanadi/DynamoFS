import cPickle

class ServerStub:
    kvstore = dict()
    backup_filename = ""

    def __init__(self, backup_filename):
        self.backup_filename = backup_filename
        try:
            self.kvstore = cPickle.load(open(backup_filename, 'r'))
        except:
            # in case no server stub backup
            pass

    def __del__(self):
        cPickle.dump(self.kvstore, open(self.backup_filename, 'w'))

    def put(self, key, value):
        self.kvstore[key] = value

    def get(self, key):
        return self.kvstore[key]
