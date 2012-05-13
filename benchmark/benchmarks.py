'''
Code to perform official benchmarks for the paper's evaluation section.
'''

from random import randint
import sys
import benchmark_utils
import dynamo_fs
import os
from dynamo_fs import concatPath
import file
import shutil

# General Note: fileSizes will be rounded up to the nearest multiple of
# chunk size, for all benchmarks.
CHUNK_SIZE = 4096

# Generate some random data to read and write.
STRING = benchmark_utils.randomString(CHUNK_SIZE)
ARRAY = benchmark_utils.randomArray(CHUNK_SIZE)

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
    
    if f.stringOptimized:
        print 'WARNING: Data not being twiddled between blocks'

    bytesLeft = fileSize
    while bytesLeft > 0:
        if rand: # Seek to a random place.
            f.seek(randPos(fileSize), file.SEEK_SET)

        if f.stringOptimized:
            f.write(STRING)
        else:
            benchmark_utils.randomArrayMutate(ARRAY)
            f.write_array(ARRAY)
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

# Populates a filesystem with a tree of randomly generated files. All the contents
# of a directory (files or directories) are named using integer values, starting at
# 0.
def makeRandomTree(fs, root, depth, fanout, fileSize):
    if depth > 0:
        for i in range(0, fanout):
            iStr = str(i)
            fs.mkdir(root, iStr)
            makeRandomTree(fs, concatPath(root, iStr), depth - 1, fanout, fileSize)
    else: # depth is zero; now we make files.
        for i in range(0, fanout):
            f = fs.open(concatPath(root, str(i)), 'w')
            
            bytesLeft = fileSize
            while bytesLeft > 0:
                if f.stringOptimized:
                    f.write(benchmark_utils.randomString(CHUNK_SIZE))
                else:
                    f.write_array(benchmark_utils.randomArray(CHUNK_SIZE))
                bytesLeft -= CHUNK_SIZE
                
            f.close()
            
# Randomly mutates files in a tree created by makeRandomTree.
def mutateRandomTree(fs, root, depth, fanout, numMutations):
    for _ in range(0, numMutations):
        # Create a random path.
        path = root
        for _ in range(0, depth + 1): # Add 1 to create the filename at the end of the directory string.
            path = concatPath(path, str(randint(0, fanout - 1)))

        # Write a chunk to the file.
        f = fs.open(path, 'w')
        if f.stringOptimized:
            f.write(benchmark_utils.randomString(CHUNK_SIZE))
        else:
            f.write_array(benchmark_utils.randomArray(CHUNK_SIZE))
            
        f.close()

# Performs all four benchmarks in the following order:
#   sequential write
#   random write
#   sequential read
#   random read
# Returns a dict containing the time readings from all four benchmarks.
def runAllWithFile(idstring):

    def benchWriteSeq(fs, filename, depth, fileSize):
        sampler = benchmark_utils.BenchmarkTimer()
        return (
            'seqwrite',
            idstring,
            depth,
            fileSize,
            write(fs, filename, fileSize, sampler, False).mean()
        )
    def benchWriteRand(fs, filename, depth, fileSize):
        sampler = benchmark_utils.BenchmarkTimer()
        return (
            'randwrite',
            idstring,
            depth,
            fileSize,
            write(fs, filename, fileSize, sampler, True).mean()
        )
    def benchReadSeq(fs, filename, depth, fileSize):
        sampler = benchmark_utils.BenchmarkTimer()
        return (
            'seqread',
            idstring,
            depth,
            fileSize,
            read(fs, filename, fileSize, sampler, False).mean()
        )
    def benchReadRand(fs, filename, depth, fileSize):
        sampler = benchmark_utils.BenchmarkTimer()
        return (
            'randread',
            idstring,
            depth,
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
    for _ in range(numTrials):
        for bench in runAllWithFile(idstring):
            del fs
            fs = fsClass()
            benchmark_utils.clearFSCache()
            results.append(bench(fs, filename, depth, fileSize))
            fs.flush()
    return results

# Ensures that a given file does not exist.
def ensureDelete(filename):
    try:
        os.unlink(filename)
    except:
        pass
    
# Runs all four benchmarks on BerkeleyDB.
def runBerkeleyDB(depth, fileSize, numTrials = 10):
    from berkeleydb_backend import BerkeleyDBBackend
    
    backingFile = 'benchmark/temp/bench.db'
    fsRootFile =  'benchmark/temp/fs_root.txt'
    ensureDelete(backingFile)
    ensureDelete(fsRootFile)

    def fsClass():
        backend = BerkeleyDBBackend(backingFile)
        return dynamo_fs.DynamoFS(backend, fsRootFile)

    return runAllWithFs(fsClass, depth, fileSize, "berkeleydb", numTrials)

# Runs all four benchmarks on DynamoDB.
def runDynamoDB(depth, fileSize, numTrials = 10):
    from dynamodb_backend import DynamoDBBackend
    
    fsRootFile =  'benchmark/temp/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = DynamoDBBackend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "dynamodb", numTrials)

# Runs all four benchmarks on DynamoDB.
def runDynamoDB_Depth(fileSize, maxDepth=15):
    from dynamodb_backend import DynamoDBBackend
    
    fsRootFile =  'benchmark/temp/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = DynamoDBBackend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    NUM_TRIALS = 5
    results = list()
    
    for depth in range(0, maxDepth + 1):
        print "Depth = " + str(depth)
        results += (runAllWithFs(fsClass, depth, fileSize, "dynamodb", NUM_TRIALS))
    return results

# Runs all four benchmarks on SimpleDB.
def runSimpleDB(depth, fileSize, numTrials = 10):
    from simpledb_backend import SimpleDBBackend
    
    fsRootFile =  'benchmark/temp/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = SimpleDBBackend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "simpledb", numTrials)

# Runs all four benchmarks on S3.
def runS3(depth, fileSize, numTrials = 10):
    from s3_backend import S3Backend
    
    fsRootFile =  'benchmark/temp/fs_root.txt'
    ensureDelete(fsRootFile)

    def fsClass():
        backend = S3Backend()
        return dynamo_fs.DynamoFS(backend, fsRootFile)
    
    return runAllWithFs(fsClass, depth, fileSize, "s3", numTrials)

# Runs all four benchmarks on a LocalFS.
def runLocalFS(depth, fileSize, numTrials = 10):
    from local_fs import LocalFS
    
    root = 'benchmark/temp/localfs'
    try:
        shutil.rmtree(root) # Nuke the local fs.
    except:
        pass

    def fsClass():
        return LocalFS(root)

    return runAllWithFs(fsClass, depth, fileSize, "localfs", numTrials)

def runLocalBenchmarks(numTrials=10, maxSize=500000):
    for fileSize in range(maxSize, 0,  -maxSize / 20):
        print >> sys.stderr, "Running LocalFS, file size %s" % fileSize
        print "\n".join(
            [",".join(map(str, row)) for row in runLocalFS(3, fileSize, numTrials)]
        )
        print >> sys.stderr, "Running BerkeleyDB, file size %s" % fileSize
        print "\n".join(
            [",".join(map(str, row)) for row in runBerkeleyDB(3, fileSize, numTrials)]
        )

def main():
    if len(sys.argv) == 2:
        maxSize = int(sys.argv[1])
        runLocalBenchmarks(maxSize = maxSize)
    else:
        runLocalBenchmarks()

# Benchmarks the merge call.
def merge(fs, fsRootFile, mutations, depth, fanout):
    sampler = benchmark_utils.BenchmarkTimer()
    
    # Populate the filesystem.
    fs.mkdir('/', '_')
    makeRandomTree(fs, '/_', depth, fanout, CHUNK_SIZE * 3) # Total file size: (2^7)(16)(4096) = 8388608 bytes
    
    # Make a clone of the filesystem, first copying the root-file directory.
    shutil.copy(fsRootFile, fsRootFile + '.copy')
    fs2 = dynamo_fs.DynamoFS(fs.get_backend(), fsRootFile + '.copy')

    # Mutate both copies with 128 random mutations.
    mutateRandomTree(fs, '/_', depth, fanout, mutations)
    mutateRandomTree(fs2, '/_', depth, fanout, mutations)
    
    # Merge the two filesystems back together.
    key = fs.get_key_for_sharing('/_')
    
    sampler.begin()
    fs2.merge_with_shared_key('/_', key)
    fs2.cleanup()
    sampler.end()
    
    return sampler

# Benchmarks the merge call.
def mergeLocal(fs, localfsroot, localfs2root, mutations, depth, fanout):
    sampler = benchmark_utils.BenchmarkTimer()
    
    from local_fs import LocalFS

    # Populate the filesystem.
    fs.mkdir('/', '_')
    makeRandomTree(fs, '/_', depth, fanout, CHUNK_SIZE * 3) # Total file size: (2^7)(16)(4096) = 8388608 bytes
    
    # Make a clone of the filesystem, first copying the root-file directory.
    shutil.copytree(localfsroot, localfs2root)
    fs2 = LocalFS(localfs2root)

    # Mutate both copies with 128 random mutations.
    mutateRandomTree(fs, '/_', depth, fanout, mutations)
    mutateRandomTree(fs2, '/_', depth, fanout, mutations)
    
    import subprocess
    sampler.begin()
    subprocess.check_output('cp -r %s/* %s' % (localfs2root, localfsroot), shell=True)
    sampler.end()
    
    return sampler

if __name__ == '__main__':
    main()
