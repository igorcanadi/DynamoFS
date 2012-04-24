# Timing and other utilities for micro-benchmarking.

import time
import dynamo_fs
import os

# Collects timing samples during benchmarking.
class BenchmarkTimer:
    def __init__(self):
        self.samples = []
        
    def begin(self):
        self.beginTime = time.time()
        
    def end(self):
        endTime = time.time()
        delta = endTime - self.beginTime
        self.samples.append(delta)
        
    def mean(self):
        return sum(self.samples) / len(self.samples)

# Creates a clean, empty DynamoFS object.
def emptyFs(backend, rootFilename):
    # Clean up any leftovers from the last test.
    backend.nuke()
    try:
        os.unlink(rootFilename)
    except:
        pass
    return dynamo_fs.DynamoFS(backend, rootFilename)