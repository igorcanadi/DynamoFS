'''
Standard benchmarking script for DynamoDB and S3 backends.
'''
import benchmarks

DEPTH = 0
FILE_SIZE = 4 * 4096 # Four blocks = 16K

def runDynamoDB():
    benchmarks.runDynamoDB(DEPTH, FILE_SIZE)
    
def runS3():
    benchmarks.runS3(DEPTH, FILE_SIZE)