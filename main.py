from controller import Controller
import dynamo_fs
import server_stub
import blob 
import hashlib
import cPickle

def generate_root(cntl):
    b = blob.DirectoryBlob(None, cntl, None, True)
    b.flush()
    open('fs_root.txt', 'w').write(b.key)
    return b

if __name__ == '__main__':
    ss = server_stub.ServerStub('server_stub_backup.dat')
    dfs = dynamo_fs.DynamoFS(ss, 'fs_root.txt')
    dfs.root = generate_root(dfs.cntl)
    dfs.mkdir('/', 'test_dir')
    dfs.mkdir('/', 'works')
    dfs.mkdir('/test_dir', 'win')
    print dfs.ls('/test_dir')
    print dfs.ls('/')
    print dfs.ls('/test_dir')
    print dfs.ls('/test_dir')


