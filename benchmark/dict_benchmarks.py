# Runs benchmarks on a DictBackend.

from dict_backend import DictBackend
import bench_append

backend = DictBackend('./backend.db')

depth = 0
samples = 100
size = 100
results = bench_append.run(backend, depth, samples, size)

print str(results)