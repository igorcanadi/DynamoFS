import bsddb
import hashlib
import os
import shutil
import sys
import time
from benchmark_utils import randomString
from file import PAGE_SIZE

__author__ = 'jpaton'

def printUsage():
    print "Usage: %s <max file size>" % sys.argv[0]

def main():
    if len(sys.argv) != 2:
        printUsage()
        sys.exit(1)
    maxSize = int(sys.argv[1])
    for size in range(maxSize, 0, -maxSize / 20):
        db = bsddb.hashopen("bdb.db")
        totalWritten = 0
        beginTime = time.time()
        for block in range((size + PAGE_SIZE - 1) / PAGE_SIZE):
            toWrite = min(PAGE_SIZE, size - totalWritten)
            string = randomString(toWrite)
            hash = hashlib.sha512(string).hexdigest()
            db[hash] = string
            totalWritten += toWrite
        db.close()
        endTime = time.time()
        delta = endTime - beginTime
        print "Total written: " + str(totalWritten) + " in " + str(delta)

if __name__ == '__main__':
    main()