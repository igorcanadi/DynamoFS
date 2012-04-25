# Runs benchmarks on a DictBackend. This is just a batch script for running
# at the terminal; feel free to edit as you need.

#from dict_backend import DictBackend
#from sqlite_backend import SQLiteBackend
from berkeleydb_backend import BerkeleyDBBackend
#import dynamodb_backend
import bench_append
import bench_mkdir_ls
from benchmark_utils import *

backingFile = 'benchmarks.db'
backend = BerkeleyDBBackend(backingFile)

fs = emptyFs(backend, 'benchmark/data/fs_root.txt')

#cap = backend.capacityUsed
print '*** Append benchmark ***'
depth = 10
samples = 10
size = 50
results = bench_append.run(fs, depth, samples, size)
print 'Mean append time: ' + str(results.mean()) + ' sec'
#print 'Capacity used: ' + str(backend.capacityUsed - cap)

fs = emptyFs(backend, 'benchmark/data/fs_root.txt')

#cap = backend.capacityUsed
print '*** Mkdir benchmark ***'
depth = 5
spread = 4
mkdirResults, lsResults = bench_mkdir_ls.run(fs, depth, spread)
print 'Mean mkdir time: ' + str(mkdirResults.mean()) + ' sec'
print 'Mean ls time: ' + str(lsResults.mean()) + ' sec'
#print 'Capacity used: ' + str(backend.capacityUsed - cap)

""" # Only useful for DynamoDB backends:
print '### Backend statistics:'
print ('Mean request latency (std-dev): ' + str(dynamodb_backend.sampler.mean()) +
       ' (' + str(dynamodb_backend.sampler.stddev()) + ')')
"""