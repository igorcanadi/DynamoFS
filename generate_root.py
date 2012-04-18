import controller
import dict_backend
import sqlite_backend
import sys
import blob

def generate_root(root_filename, server):
    cntl = controller.Controller(server, root_filename)
    b = blob.DirectoryBlob(None, cntl, None, True)
    b.flush()
    open(root_filename, 'w').write(b.key)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "usage: %s root_filename server_type server_filename" % sys.argv[0]
        exit(1)

    if sys.argv[2] == 'dict':
        server = dict_backend.DictBackend(sys.argv[3])
    elif sys.argv[2] == 'sqlite':
        server = sqlite_backend.SQLiteBackend(sys.argv[3])
    else:
        print "server_type E {'dict', 'sqlite'}"
        exit(1)

    generate_root(sys.argv[1], server)


