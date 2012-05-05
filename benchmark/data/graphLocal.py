#!/usr/bin/env python

import sys
from matplotlib import pyplot as plt
import numpy as np

__author__ = 'jpaton'

"""
This file will eventually produce graphs for the local benchmarks
"""

import csv

def printUsage():
    print """\
Usage: %s <csv file name>\
""" % sys.argv[0]

"""
types
Dict mapping (benchtype, fstype) to a string that shall be used in the legend
of the plot
"""
types = {
    ("seqwrite", "localfs") : "Sequential write",
    ("seqread", "localfs") : "Sequential read",
    ("randwrite", "localfs") : "Random write",
    ("randread", "localfs") : "Random read",
    ("seqwrite", "berkeleydb") : "Sequential write",
    ("seqread", "berkeleydb") : "Sequential read",
    ("randwrite", "berkeleydb") : "Random write",
    ("randread", "berkeleydb") : "Random read",
}

legendStrings = [
    "Sequential write",
    "Sequential read",
    "Random write",
    "Random read",
]

def sortByType(data):
    """
    Returns a tuple as follows: (
        [seqwrite, localfs],
        [seqread, localfs],
        [randwrite, localfs],
        [randread, localfs],
        [seqwrite, berkeleydb],
        [seqread, berkeleydb],
        [randwrite, berkeleydb],
        [randread, berkeleydb],
    )
    """
    retval = ([list() for i in range(8)])
    for item in data:
        if (item['benchtype'], item['fstype']) == ('seqwrite', 'localfs'):
            retval[0].append(item)
        elif (item['benchtype'], item['fstype']) == ('seqread', 'localfs'):
            retval[1].append(item)
        elif (item['benchtype'], item['fstype']) == ('randwrite', 'localfs'):
            retval[2].append(item)
        elif (item['benchtype'], item['fstype']) == ('randread', 'localfs'):
            retval[3].append(item)
        elif (item['benchtype'], item['fstype']) == ('seqwrite', 'berkeleydb'):
            retval[4].append(item)
        elif (item['benchtype'], item['fstype']) == ('seqread', 'berkeleydb'):
            retval[5].append(item)
        elif (item['benchtype'], item['fstype']) == ('randwrite', 'berkeleydb'):
            retval[6].append(item)
        elif (item['benchtype'], item['fstype']) == ('randread', 'berkeleydb'):
            retval[7].append(item)
        else:
            raise Exception()
    return retval

def getAverages(data):
    """
    Returns dict of sizes (type str) to average time (type float)
    """
    retval = dict()
    for item in data:
        size = item['size']
        time = item['time']
        if size not in retval:
            retval[size] = (0, 0) # (total time, number of samples)
        totaltime, num = retval[size]
        retval[size] = (totaltime + float(time), num + 1)
    for size, (totaltime, num) in retval.iteritems():
        retval[size] = totaltime / num
    return retval

def sortedTuples(data):
    """
    data: dict mapping size:str to time:float
    returns: list of (size:str, time:float) tuples sorted by int(size)
    """
    return sorted(data.items(), lambda x, y: int(x[0]) - int(y[0]))

def main():
    if len(sys.argv) != 2:
        printUsage()
        sys.exit(1)

    filename = sys.argv[1]
    fieldnames = [
        "benchtype",
        "fstype",
        "depth",
        "size",
        "time",
    ]
    reader = csv.DictReader(open(filename, 'r'), fieldnames = fieldnames)

    data = sortByType(reader)
    ind = np.arange(20) + 0.1
    fig = plt.figure(figsize = (8, 4), dpi = 600)
    ax = fig.add_subplot(111)
    barwidth = 0.2
    rects = list()
    colors = ['r', 'g', 'y', 'b']
    xlabels = map(lambda x: int(x[0]) / 10**6, sortedTuples(getAverages(data[0])))[1::2]
    print xlabels
    for i in range(4):
        toPlot = map(lambda x: float(x[0]) / x[1] / 10**6, sortedTuples(getAverages(data[i + 4])))
        rects.append(ax.bar(
            ind + i * barwidth,
            toPlot,
            width = barwidth,
            color = colors[i],
        ))

    ax.set_xticks((ind + barwidth * 2)[1::2])
    ax.set_xticklabels(xlabels)
    ax.set_ylabel("Throughput (MB)")
    ax.set_xlabel("File size (MB)")
    ax.legend(rects, legendStrings, loc="best")
    plt.savefig("berk_ebs.svg")

if __name__ == '__main__':
    main()