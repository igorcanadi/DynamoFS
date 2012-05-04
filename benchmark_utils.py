# Timing and other utilities for micro-benchmarking.

from array import array
import shutil
from subprocess import call
import time
import dynamo_fs
import os
import random
import math

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
    
    def stddev(self):
        mean = self.mean()
        variance = 0.0
        for s in self.samples:
            variance += (s - mean) ** 2.0
        return math.sqrt(variance)

# Creates a clean, empty DynamoFS object.
def emptyFs(backend, rootFilename):
    # Clean up any leftovers from the last test.
    backend.nuke()
    try:
        os.unlink(rootFilename)
    except:
        pass
    return dynamo_fs.DynamoFS(backend, rootFilename)

# Creates an n-deep nesting of directories, starting at the given root.
# The path to the deepest directory is returned.
def makeDepth(fs, root, n):
    cwd = root
    for _ in range(0, n):
        # Make up a random 5-character name for this directory.
        name = randomDirName()
        fs.mkdir(cwd, name)
        cwd = dynamo_fs.concatPath(cwd, name)
    return cwd

def randomArray(length, rangeMin = 32, rangeMax = 126):
    ar = array('B')
    for _ in range(0, length):
        ar.expand(random.randint(rangeMin, rangeMax))
    return ar

# Generates a random string of printable ASCII. Optionally, a sub-range of
# printable ASCII may be specified using min and max.
def randomString(length, rangeMin = 32, rangeMax = 126):
    text = ''
    for _ in range(0, length):
        text += chr(random.randint(rangeMin, rangeMax))
    return text
    
# Generates a random string of lower-case letters.
def randomAlphaString(length):
    return randomString(length, ord('a'), ord('z'))

# Creates a normally-sized random directory name.
def randomDirName():
    return randomAlphaString(7)

# Generates a random string of printable ASCII. All the characters will
# be the same randomly chosen character.
def semirandomString(length, rangeMin = 32, rangeMax = 126):
    return chr(random.randint(rangeMin, rangeMax)) * length

def semirandomArray(length, rangeMin = 32, rangeMax = 126):
    return array('b', [random.randint(rangeMin, rangeMax)] * length)

# Takes in a list of tuples (or a list of lists) and prints it out to stdout
# in a CSV format.
def printCsv(data):
    for line in data:
        print ','.join(map(str, line))

# Clears the filesystem cache. Only works on Linux.
# Based on http://www.linuxquestions.org/questions/linux-kernel-70/how-to-disable-filesystem-cache-627012/
def clearFSCache():
    call(['sync'])
    call(['echo', '3', '>', '/proc/sys/vm/drop_caches'])