# Benchmark for appending to files.

import timing
import dynamo_fs
import random
import os
from file import SEEK_END

# backend - a backend to use
# depth - depth at which to work. This controls how many directories deep
#         we store the file that receives the append traffic.
# samples - number of appends to do
# size - size (in bytes) of each append
def run(backend, depth, samples, size):
    rootFilename = 'benchmark/data/fs_root.txt'
    
    # Clean up any leftovers from the last test.
    backend.nuke()
    try:
        os.unlink(rootFilename)
    except:
        pass
    
    dfs = dynamo_fs.DynamoFS(backend, rootFilename)
    
    # Set up the test by creating a chain of directories to work in.
    cwd = '/'
    for i in range(0, depth):
        # Make up a random 5-character name for this directory.
        name = str(random.randint(10000, 99999))
        dfs.mkdir(cwd, name)
        cwd += '/' + name
    
    # Perform benchmarking.    
    sampler = timing.BenchmarkTimer()
    filename = cwd + '/file'
    
    for i in range(0, samples):
        # Generate a random (printable) string to write.
        text = ''
        for j in range(0, size):
            text += chr(random.randint(32, 126))
    
        # For each sample, open the file, seek to the end, and write some bytes.
        sampler.begin()
        f = dfs.open(filename, 'w')
        f.seek(0, SEEK_END)
        f.write(text)
        f.close()
        sampler.end()
        
    # Done.
    return sampler.samples
        
        
    