# Runs benchmarks on a DictBackend. This is just a batch script for running
# at the terminal; feel free to edit as you need.

from dict_backend import DictBackend
from sqlite_backend import SQLiteBackend
#from dynamodb_backend import DynamoDBBackend
import bench_append
import bench_mkdir_ls
import time

backingFile = 'benchmarks.db'
backend = DictBackend(backingFile)

start = time.time()

""" # Append benchmark. 
depth = 10
samples = 10
size = 100
results = bench_append.run(backend, depth, samples, size)
print 'Mean append time: ' + str(results.mean()) + ' sec'
"""

# Mkdir benchmark.
depth = 5
spread = 5
mkdirResults, lsResults = bench_mkdir_ls.run(backend, depth, spread)
print 'Mean mkdir time: ' + str(mkdirResults.mean()) + ' sec'
print 'Mean ls time: ' + str(lsResults.mean()) + ' sec'

end = time.time()


print 'Total test time: ' + str(end - start) + ' sec'
