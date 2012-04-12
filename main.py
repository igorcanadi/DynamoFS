import dynamo_fs
import server_stub
import generic_blob
import directory_blob
import hashlib
import cPickle

if __name__ == '__main__':
    ss = server_stub.ServerStub('server_stub_backup.dat')
    print ss.get('a')
    db = directory_blob.DirectoryBlob()
    db.reference_count = 1
    gb = generic_blob.GenericBlob()
    gb.construct_from_object(db)
    (h, b) = gb.get_hash_and_blob()
    ss.put(h, b)

    open('fs_root.txt', 'w').write(h)
