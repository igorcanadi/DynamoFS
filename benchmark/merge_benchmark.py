'''
Standard benchmark for merging trees.
'''

from berkeleydb_backend import BerkeleyDBBackend
from dynamo_fs import DynamoFS
from benchmarks import merge, mergeLocal
from local_fs import LocalFS
import shutil

backend = BerkeleyDBBackend('benchmark/temp/bench.db')
fsRootFile = 'benchmark/temp/fs_root.txt'
fs = DynamoFS(backend, fsRootFile)

sampler = merge(fs, fsRootFile, 128, 11, 2)

print sampler.samples

root = 'benchmark/temp/localfs'
root2 = 'benchmark/temp/localfs2'
shutil.rmtree(root)
shutil.rmtree(root2)
local_fs = LocalFS(root)

sampler_local = mergeLocal(local_fs, root, root2, 128, 11, 2)

print sampler_local.samples
