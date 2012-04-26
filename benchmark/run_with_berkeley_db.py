from benchmarks import *
import sys

if len(sys.argv) < 2:
    print "usage: %s size" % sys.argv[0]
    exit(1)

print runAllWithBerkeleyDB(1, int(sys.argv[1]))
