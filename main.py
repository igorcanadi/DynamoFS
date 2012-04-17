from controller import Controller, generate_root
import dynamo_fs
import server_stub
import blob 
import hashlib
import cPickle

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


