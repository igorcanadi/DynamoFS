# Runs benchmarks on a DictBackend. This is just a batch script for running
# at the terminal; feel free to edit as you need.

from dict_backend import DictBackend
from sqlite_backend import SQLiteBackend
from dynamodb_backend import DynamoDBBackend
import bench_append
import time

backingFile = 'benchmarks.db'
backend = DictBackend(backingFile)

depth = 10
samples = 10
size = 100

start = time.time()
results = bench_append.run(backend, depth, samples, size)
end = time.time()

print 'Mean append time: ' + str(results.mean()) + ' sec'
print 'Total test time: ' + str(end - start) + ' sec'