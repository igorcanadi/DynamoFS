import dynamo_fs
import server_stub
import blob 
import hashlib
import cPickle

def generate_root(server):
    b = blob.Blob.generate_from_type(0)
    (h, b) = b.get_hash_and_blob()
    ss.put(h, b)
    open('fs_root.txt', 'w').write(h)

if __name__ == '__main__':
    ss = server_stub.ServerStub('server_stub_backup.dat')
    dfs = dynamo_fs.DynamoFS(ss, 'fs_root.txt')
    dfs.mkdir('/', 'test_dir')
    dfs.mkdir('/', 'works')
    dfs.mkdir('/test_dir', 'win')
    print dfs.ls('/test_dir')
    print dfs.ls('/')

