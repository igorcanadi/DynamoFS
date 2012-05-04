#!/usr/bin/env python

import sys
import numpy
import csv
import os
import re
import pylab

BENCH_TYPES = ['seqwrite', 'seqread', 'randwrite', 'randread'];

def extractMeans(csvData):
    """
    Extracts the mean of all samples for each benchmark (like seqwrite or randread)
    Returns a map of benchmark names to average time samples.
    """
    sums = {
        'seqwrite':0,
        'seqread':0,
        'randwrite':0,
        'randread':0
    }
    count = 0
    for record in csvData:
        sums[record['benchtype']] += 1/float(record['time'])
        count += 1
    for benchType in BENCH_TYPES:
        sums[benchType] /= count
    return sums


def main():
    home = 'dynamodb-ec2'
    fieldnames = [
        "benchtype",
        "fstype",
        "depth",
        "size",
        "time",
    ]

    # Keep four lists of data for the four benchmarks, plus one list for the
    # x-axis (write provisioning).
    xData = []
    yData = {
        'seqwrite':[],
        'seqread':[],
        'randwrite':[],
        'randread':[]
    }

    fPattern = re.compile(r"(\d+)-r(\d+)-w(\d+).csv")
    for f in os.listdir(home):
        matches = fPattern.match(f)
        if matches is not None:
            timestamp = matches.group(1)
            readUnits = matches.group(2)
            writeUnits = matches.group(3)
            means = extractMeans(csv.DictReader(open(home + '/' + f, 'r'), fieldnames=fieldnames))
            
            xData.append(float(writeUnits))
            for benchType in BENCH_TYPES:
                yData[benchType].append(means[benchType])
                
    # TODO average samples for each throughput.

    print xData
    print yData['seqwrite']
    pylab.scatter(xData, yData['seqwrite'])
    pylab.show()

if __name__ == '__main__':
    main()
