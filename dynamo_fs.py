import server_stub
import file

class DynamoFS:
    root_filename = ""
    root_hash = ""

    def mount(self, root_filename):
        self.root_filename = root_filename
        self.root_hash = open(root_filename, 'r').read().split('\n')[0]
        pass

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        pass

    def rm(self, filename):
        pass

    def rmdir(self, path):
        pass

    def rename(self, filename, new_filename):
        pass

