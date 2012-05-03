'''
Standard benchmarking script for DynamoDB and S3 backends.
'''
import benchmarks
from benchmark_utils import printCsv

DEPTH = 0
FILE_SIZE = 4 * 4096 # Four blocks = 16K

results = benchmarks.runDynamoDB(DEPTH, FILE_SIZE)
printCsv(results)
