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
    f.write('ja sam mali pas')
    f.close()

    dfs.debug_output_whole_tree()
    k = dfs.get_key_for_sharing('/test_dir')
    dfs.attach_shared_key('/test_dir/', 'new_test_dir', k)
    dfs.debug_output_whole_tree()
    f = dfs.open('/test_dir/new_test_dir/pas', 'w')
    f.write('ja sam mali majmun')
    dfs.debug_output_whole_tree()

    dfs.cleanup()
    ss.cleanup()
