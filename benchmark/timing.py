# Timing utilities for micro-benchmarking.

import time

# Collects timing samples during benchmarking.
class BenchmarkTimer:
    def __init__(self):
        self.samples = []
        
    def begin(self):
        self.startTime = time.clock()
        
    def end(self):
        endTime = time.clock()
        delta = endTime - self.beginTime
        self.samples.append(delta)