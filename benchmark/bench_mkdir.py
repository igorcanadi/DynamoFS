# Benchmark for creating directories. A complete tree is created with a given
# depth and fanout per node.

from benchmark_utils import *

# backend - a backend to use
# depth - depth of the directory tree to create
# breadth - number of children to populate in each directory in the tree.
def run(backend, depth, breadth):
    fs = emptyFs(backend, 'benchmark/data/fs_root.txt')   
    sampler = BenchmarkTimer()
    
    populate(fs, '/', depth, breadth, sampler)
    return sampler

# Recursively populates the tree in a depth-first ordering.
def populate(fs, parent, depthLeft, breadth, sampler):
    for i in range(0, breadth):
        child = randomDirName() + str(i) # Make sure directory names are unique.
        
        sampler.begin()
        fs.mkdir(parent, child)
        sampler.end()
        
        if depthLeft > 1:
            populate(fs, dynamo_fs.concatPath(parent, child),
                     depthLeft - 1, breadth, sampler)
            