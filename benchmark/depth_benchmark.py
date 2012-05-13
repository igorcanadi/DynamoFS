'''
Standard benchmarking script for effects of depth on DynamoDB.
'''
import benchmarks
from benchmark_utils import printCsv

# TODO
DEPTH = 0
FILE_SIZE = 4 * 4096 # Four blocks = 16K

results = benchmarks.runDynamoDB_Depth(FILE_SIZE)
printCsv(results)
