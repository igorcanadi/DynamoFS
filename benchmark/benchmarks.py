'''
Code to perform official benchmarks for the paper's evaluation section.
'''

from random import randint
import benchmark_utils
import dynamo_fs
import os
import file
import shutil

# General Note: fileSizes will be rounded up to the nearest multiple of
# chunk size, for all benchmarks.
CHUNK_SIZE = 4096

# Generates a random position to seek to in a file.
def randPos(fileSize):
    if fileSize <= CHUNK_SIZE:
        return 0
    else:
        return randint(0, fileSize - 1 - CHUNK_SIZE)

# rand - True for random writes, false for sequential writes.
def write(fs, filename, fileSize, sampler, rand):
    sampler.begin()
    f = fs.open(filename, 'w')

    bytesLeft = fileSize
    while bytesLeft > 0:
        if rand: # Seek to a random place.
            f.seek(randPos(fileSize), file.SEEK_SET)

        if f.stringOptimized:
            f.write(benchmark_utils.semirandomString(CHUNK_SIZE))
        else:
            f.write_array(benchmark_utils.semirandomArray(CHUNK_SIZE))
        bytesLeft -= CHUNK_SIZE

    f.close()

    # Flush the filesystem caches before finishing the timer.
    fs.flush()

    sampler.end()
    return sampler

# rand - True for random reads, false for sequential reads.
def read(fs, filename, fileSize, sampler, rand):
    # Make sure to start with an empty filesystem cache.
    fs.flush()

    sampler.begin()
    f = fs.open(filename, 'r')

    bytesLeft = fileSize
    while bytesLeft > 0:
        if rand: # Seek to a random place.
            f.seek(randPos(fileSize), file.SEEK_SET)

        if f.stringOptimized:
            f.read(CHUNK_SIZE)
        else:
            f.read_array(CHUNK_SIZE)
        bytesLeft -= CHUNK_SIZE

    f.close()
    sampler.end()
    return sampler

# Performs all four benchmarks in the following order:
#   sequential write
#   random write
#   sequential read
#   random read
# Returns a dict containing the time readings from all four benchmarks.
def runAllWithFile(idstring):
    sampler = benchmark_utils.BenchmarkTimer()

    def benchWriteSeq(fs, filename, fileSize):
        return (
            'seqwrite',
            idstring,
            fileSize,
            write(fs, filename, fileSize, sampler, False).mean()
        )
    def benchWriteRand(fs, filename, fileSize):
        return (
            'randwrite',
            idstring,
            fileSize,
            write(fs, filename, fileSize, sampler, True).mean()
        )
    def benchReadSeq(fs, filename, fileSize):
        return (
            'seqread',
            idstring,
            fileSize,
            read(fs, filename, fileSize, sampler, False).mean()
        )
    def benchReadRand(fs, filename, fileSize):
        return (
            'randread',
            idstring,
            fileSize,
            read(fs, filename, fileSize, sampler, True).mean()
        )

    yield benchWriteSeq
    yield benchWriteRand
    yield benchReadSeq
    yield benchReadRand

# Runs all four benchmarks on a file in a randomly generated directory.
def runAllWithFs(fsClass, depth, fileSize, idstring, numTrials=10):
    # Create a random chain of directories to get the proper depth.
    fs = fsClass()
    cwd = benchmark_utils.makeDepth(fs, '/', depth)
    filename = dynamo_fs.concatPath(cwd, 'the_file')
    fs.flush()
    results = list()
    for i in range(numTrials):
        for bench in runAllWithFile(idstring):
            fs = fsClass()
            results.append(bench(fs, filename, fileSize))
            fs.flush()
    return results
#    return runAllWithFile(fs, filename, fileSize)

# Ensures that a given file does not exist.
def ensureDelete(filename):
    try:
        os.unlink(filename)
    except:
        pass
    
# Runs all four benchmarks on BerkeleyDB.
def runBerkeleyDB(depth, fileSize):
    from berkeleydb_backend import BerkeleyDBBackend
    
    backingFile = 'benchmark/data/bench.db'
    fsRootFile =  'benchmark/data/fs_root.txt'
    ensureDelete(backingFile)
    ensureDelete(fsRootFile)

    def fsClass():
        backend = BerkeleyDBBackend(backingFile)
        return dynamo_fs.DynamoFS(backend, fsRootFile)

    return runAllWithFs(fsClass, depth, fileSize, "berkeleydb")

# Runs all four benchmarks on a DictBackend.
#def runDict(depth, fileSize):
#    from dict_backend import DictBackend
#
#    backingFile = 'benchmark/data/bench.db'
#    fsRootFile =  'benchmark/data/fs_root.txt'
#    ensureDelete(backingFile)
#    ensureDelete(fsRootFile)
#
#    def fsClass():
#        backend = DictBackend(backingFile)
#        return dynamo_fs.DynamoFS(backend, fsRootFile)
#
#    return runAllWithFs(fsClass, depth, fileSize, "dict")

# Runs all four benchmarks on BerkeleyDB.
def runDynamoDB(depth, fileSize):
    from dynamodb_backend import DynamoDBBackend
    
    fsRootFile =  'benchmark/data/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = DynamoDBBackend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "dynamodb")

# Runs all four benchmarks on BerkeleyDB.
def runSimpleDB(depth, fileSize):
    from simpledb_backend import SimpleDBBackend
    
    fsRootFile =  'benchmark/data/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = SimpleDBBackend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "simpledb")

# Runs all four benchmarks on BerkeleyDB.
def runS3(depth, fileSize):
    from s3_backend import S3Backend
    
    fsRootFile =  'benchmark/data/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = S3Backend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "s3")

# Runs all four benchmarks on a LocalFS.
def runLocalFS(depth, fileSize):
    from local_fs import LocalFS
    
    root = 'benchmark/data/localfs'
    try:
        shutil.rmtree(root) # Nuke the local fs.
    except:
        pass

    def fsClass():
        return LocalFS(root)

    return runAllWithFs(fsClass, depth, fileSize, "localfs")
