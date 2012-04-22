from controller import Controller 
import dynamo_fs
import dict_backend
#import sqlite_backend
import blob 
import hashlib
import cPickle

if __name__ == '__main__':
    ss = dict_backend.DictBackend('server_stub_backup.dat')
    #ss = sqlite_backend.SQLiteBackend('sqlite.db')
    dfs = dynamo_fs.DynamoFS(ss, 'fs_root.txt')
    dfs.mkdir('/', 'test_dir')
    dfs.mkdir('/test_dir', 'win')
    f = dfs.open('/test_dir/pas', 'w')
    print f.blob.key
    f.write('ja sam mali pas')
    f.close()

    f2 = dfs.open('/test_dir/pas', 'r')
    print f2.read(1000)
    dfs.debug_output_whole_tree()
    dfs.cleanup()

    exit(1)

    k = dfs.get_key_for_sharing('/test_dir')
    dfs.attach_shared_key('/test_dir/', 'new_test_dir', k)
    print dfs.ls('/test_dir')
    print dfs.ls('/test_dir/new_test_dir')
    dfs.cleanup()
    ss.cleanup()
    f = dfs.open('/test_dir/new_test_dir/pas', 'r')
    print f.blob.key
    print f.read()
    f = dfs.open('/test_dir/pas', 'r')
    print f.blob.key


