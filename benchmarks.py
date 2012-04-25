'''
Code to perform benchmarks for the paper's evaluation section.
'''

import random
from benchmark_utils import *

# General Note: fileSizes will be rounded up to the nearest multiple of
# chunk size, for all benchmarks.
CHUNK_SIZE = 100

# rand - True for random writes, false for sequential writes.
def write(fs, filename, fileSize, sampler, rand):
    sampler.begin()
    f = fs.open(filename, 'w')
    
    bytesLeft = fileSize
    while bytesLeft > 0:
        if rand: # Seek to a random place.
            pos = random.randint(0, fileSize - 1 - CHUNK_SIZE)
            f.seek(pos, file.SEEK_SET)
        f.write(semirandomString(CHUNK_SIZE))
        bytesLeft -= CHUNK_SIZE
        
    f.close()
    sampler.end()
    return sampler

# rand - True for random reads, false for sequential reads.
def read(fs, filename, fileSize, sampler, rand):
    sampler.begin()
    f = fs.open(filename, 'r')
    
    bytesLeft = fileSize
    while bytesLeft > 0:
        if rand: # Seek to a random place.
            pos = random.randint(0, fileSize - 1 - CHUNK_SIZE)
            f.seek(pos, file.SEEK_SET)
        f.read(CHUNK_SIZE)
        bytesLeft -= CHUNK_SIZE
        
    f.close()
    sampler.end()
    return sampler
