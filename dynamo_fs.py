import controller 
import file

class DynamoFS:
    root_filename = ""
    root_hash = ""
    server = False

    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        return file.File(filename, mode, self.cntl)

    def rm(self, filename):
        self.cntl.rm(filename)

    def mkdir(self, path, new_dir):
        self.cntl.mkdir(path, new_dir)

    def ls(self, path):
        return self.cntl.ls(path)

    # Renames a file or directory.
    def rename(self, old_name, new_name):
        pass

