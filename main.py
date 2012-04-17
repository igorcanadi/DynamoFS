from controller import Controller
import dynamo_fs
import server_stub
import blob 
import hashlib
import cPickle

if __name__ == '__main__':
    ss = server_stub.ServerStub('server_stub_backup.dat')
    dfs = dynamo_fs.DynamoFS(ss, 'fs_root.txt')
    dfs.mkdir('/', 'test_dir')
    dfs.mkdir('/', 'works')
    dfs.mkdir('/test_dir', 'win')
    f = dfs.open('/test_dir/pas', 'w')
    f.write('ja sam mali pas')
    f.close()
    f2 = dfs.open('/test_dir/pas', 'r')
    print f2.read()


