# Benchmark for appending to files.

from dict_backend import *
from berkeleydb_backend import *
from benchmark_utils import *
from file import SEEK_END

# fs - an empty filesystem to use
def run(fs, size):
    # Perform benchmarking. 
    sampler = BenchmarkTimer()
    sampler.begin()
    # write
    f = fs.open('big_file', 'w')
    chunck_size = 10000
    for i in range(size/chunck_size):
        f.write('a' * chunck_size)
    f.close()

    # read
    f = fs.open('big_file', 'r')
    for i in range(size/chunck_size):
        a = f.read(chunck_size)
    f.close()

    sampler.end()
    
    return sampler

if __name__ == '__main__':
    #backend = BerkeleyDBBackend('berkeley_db.db')
    backend = DictBackend('dict_backend.db')

    fs = emptyFs(backend, 'benchmark/data/fs_root.txt')
    s = run(fs, 5000000)
    print s.mean()

