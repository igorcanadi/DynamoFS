import controller 

class DynamoFS:
    root_filename = ""
    root_hash = ""
    server = False

    def __init__(self, server, root_filename):
        self.cntl = controller.Controller(server, root_filename)

    # mode can be 'r' or 'w'
    def open(self, filename, mode):
        pass

    def rm(self, filename):
        pass

    def mkdir(self, path, new_dir):
        self.cntl.mkdir(path, new_dir)

    def ls(self, path):
        return self.cntl.ls(path)

    # Renames a file or directory.
    def rename(self, old_name, new_name):
        pass

