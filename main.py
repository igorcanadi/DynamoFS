import dynamo_fs
import server_stub
import directory_blob
import hashlib
import cPickle

if __name__ == '__main__':
    ss = server_stub.ServerStub('server_stub_backup.dat')
    print ss.get('a')
    ss.put('a', 'v')
    db = directory_blob.DirectoryBlob()
    db.reference_count = 1
    dbpickle = cPickle.dumps(db)
    ss.put(hashlib.sha512(dbpickle).hexdigest(), dbpickle)
    open('fs_root.txt', 'w').write(hashlib.sha512(dbpickle).hexdigest())


