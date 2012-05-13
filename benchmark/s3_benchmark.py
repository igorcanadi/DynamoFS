'''
Standard benchmarking script for S3 backend.
'''
import benchmarks
from benchmark_utils import printCsv

DEPTH = 0
FILE_SIZE = 4 * 4096 # Four blocks = 16K

results = benchmarks.runS3(DEPTH, FILE_SIZE)
printCsv(results)
