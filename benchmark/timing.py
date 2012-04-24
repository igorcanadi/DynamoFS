# Timing utilities for micro-benchmarking.

import time

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