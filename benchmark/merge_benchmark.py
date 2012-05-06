'''
Standard benchmark for merging trees.
'''

from berkeleydb_backend import BerkeleyDBBackend
from dynamo_fs import DynamoFS
from benchmarks import merge

backend = BerkeleyDBBackend('benchmark/temp/bench.db')
fs = DynamoFS(backend, 'benchmark/temp/fs_root.txt')

sampler = merge(fs)

print sampler.samples