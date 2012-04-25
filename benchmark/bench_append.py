# Benchmark for appending to files.

from benchmark_utils import *
from file import SEEK_END

# fs - an empty filesystem to use
# depth - depth at which to work. This controls how many directories deep
#         we store the file that receives the append traffic.
# samples - number of appends to do
# size - size (in bytes) of each append
def run(fs, depth, samples, size):
    # Set up the test by creating a chain of directories to work in.
    cwd = makeDepth(fs, '/', depth)
    filename = dynamo_fs.concatPath(cwd, 'file')
    
    # Perform benchmarking. 
    sampler = BenchmarkTimer()
    sampler.begin()
    f = fs.open(filename, 'w')
    
    for _ in range(0, samples):
        # Generate a random (printable) string to write.
        text = randomString(size)
        f.write(text)
        
    f.close()
    sampler.end()
    
    return sampler
