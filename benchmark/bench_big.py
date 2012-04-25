# Benchmark for appending to files.

from benchmark_utils import *
from file import SEEK_END

# fs - an empty filesystem to use
def run(fs, size):
    # Perform benchmarking. 
    sampler = BenchmarkTimer()
    sampler.begin()
    f = fs.open('big_file', 'w')

    chunck_size = 10000
    for i in range(size/chunck_size):
        f.write('a' * chunck_size)
    
    f.close()
    sampler.end()
    
    return sampler

if __name__ == '__main__':
    backend = BerkeleyDBBackend('berkeley_db.db')
    fs = emptyFs(backend, 'benchmark/data/fs_root.txt')
    s = run(fs, 50000)
    print s.mean()

