'''
Standard benchmark for merging trees.
'''

from berkeleydb_backend import BerkeleyDBBackend
from dynamo_fs import DynamoFS
from benchmarks import merge

backend = BerkeleyDBBackend('benchmark/temp/bench.db')
fsRootFile = 'benchmark/temp/fs_root.txt'
fs = DynamoFS(backend, fsRootFile)

sampler = merge(fs, fsRootFile)

print sampler.samples
