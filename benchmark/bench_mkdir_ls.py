# Benchmark for creating directories. A complete tree is created with a given
# depth and fanout per node.

from benchmark_utils import *

# fs - an empty filesystem to use
# depth - depth of the directory tree to create
# spread - number of children to populate in each directory in the tree.
# Returns a tuple of samplers: the first one contains data for mkdir calls; the
# second contains data for ls calls.
def run(fs, depth, spread):
    mkdirSampler = BenchmarkTimer()
    lsSampler = BenchmarkTimer()
    
    makeDirs(fs, '/', depth, spread, mkdirSampler)
    listDirs(fs, '/', lsSampler)
    return (mkdirSampler, lsSampler)

# Recursively populates the tree in a depth-first ordering.
def makeDirs(fs, parent, depthLeft, spread, sampler):
    for i in range(0, spread):
        child = randomDirName() + str(i) # Make sure directory names are unique.
        
        sampler.begin()
        fs.mkdir(parent, child)
        sampler.end()
        
        if depthLeft > 1:
            makeDirs(fs, dynamo_fs.concatPath(parent, child),
                     depthLeft - 1, spread, sampler)

# Recursively lists all the directory contents in the tree. Assumes that the
# tree contains only directories (no files).
def listDirs(fs, parent, sampler):
    sampler.begin()
    children = fs.ls(parent)
    sampler.end()
    
    for child in children:
        listDirs(fs, dynamo_fs.concatPath(parent, child), sampler)