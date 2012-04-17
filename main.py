from controller import Controller, generate_root
import dynamo_fs
import dict_backend
import sqlite_backend
import blob 
import hashlib
import cPickle

if __name__ == '__main__':
    #ss = dict_backend.DictBackend('server_stub_backup.dat')
    ss = sqlite_backend.SQLiteBackend('sqlite.db')
    dfs = dynamo_fs.DynamoFS(ss, 'fs_root.txt')
    dfs.mkdir('/', 'test_dir')
    dfs.mkdir('/', 'works')
    dfs.mkdir('/test_dir', 'win')
    print dfs.ls('/')
    print dfs.ls('/test_dir')
    f = dfs.open('/test_dir/pas', 'w')
    f.write('ja sam mali pas')
    f.close()
    f2 = dfs.open('/test_dir/pas', 'r')
    print f2.read()


