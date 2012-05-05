#!/usr/bin/env python

import sys
import numpy
import csv
import os
import re
import pylab
import matplotlib.ticker as ticker
from matplotlib.pyplot import gca

BENCH_TYPES = ['seqwrite', 'seqread', 'randwrite', 'randread'];

# We rad all the data into a table with the following columns.
COLUMNS = ['timestamp', 'benchType', 'backend', 'depth', 'fileSize',
           'readUnits', 'writeUnits', 'latency'];

# Read data into the table.
table = []
home = '.'
csvFields = ["benchType", "backend", "depth", "fileSize", "latency"]
fPattern = re.compile(r"(\d+)-r(\d+)-w(\d+).csv")

for f in os.listdir(home):
    matches = fPattern.match(f)
    if matches is not None:
        timestamp = matches.group(1)
        readUnits = matches.group(2)
        writeUnits = matches.group(3)
        
        csvReader = csv.DictReader(open(home + '/' + f, 'r'), csvFields)
        for csvLine in csvReader:
            # Build a record for the table.
            record = [int(timestamp),
                      csvLine['benchType'],
                      csvLine['backend'],
                      int(csvLine['depth']),
                      int(csvLine['fileSize']),
                      int(readUnits),
                      int(writeUnits),
                      float(csvLine['latency'])
                      ]
            table.append(record)


# Filters the data, producing an arbitrary subset of records.
def filter(table, timestamp=None, benchType=None, readUnits=None, writeUnits=None):
    measurements = []
    for record in table:
        if timestamp is not None and timestamp != record[0]:
            continue
        if benchType is not None and benchType != record[1]:
            continue
        if readUnits is not None and readUnits != record[5]:
            continue
        if writeUnits is not None and writeUnits != record[6]:
            continue
        measurements.append(record)
    return measurements

# Gets a column out of a table of data.
def project(table, column):
    # Convert column names to indices.
    column = COLUMNS.index(column)

    newTable = []
    for record in table:
        newTable.append(record[column])
    return newTable

# Condenses a set of scattered points, returning a list of x-coordinates
# without duplicates and a list of lists of corresponding y-coordinates.
def condense(xCoords, yCoords):
    # Tuple and sort the data to group x-coordinates together.
    tuples = zip(xCoords, yCoords)
    tuples.sort()
    
    # Condense and collect.
    newXCoords = []
    newYCoords = []
    yList = []
    lastX = tuples[0][0]
    for (x, y) in tuples:
        if x == lastX:
            yList.append(y)
        else:
            newYCoords.append(yList)
            newXCoords.append(x)
            yList = []
            lastX = x
            
    return (newXCoords, newYCoords)
    
def mean(data):
    return sum(data) / len(data)

def main():
    seqwriteData = filter(table, benchType='seqwrite')
    xData = project(seqwriteData, 'writeUnits')
    yData = project(seqwriteData, 'latency')
    
    # Each sample represents 10 trials.
    yData = map(lambda x:x/10, yData)
    
    (xData, yData) = condense(xData, yData)
    pylab.boxplot(yData)
    
    fmt = ticker.FixedFormatter(map(str, xData))
    ax = gca()
    ax.get_xaxis().set_major_formatter(fmt)
    ax.set_ylabel("Sequential block write latency (s)")
    ax.set_xlabel("Provisioned write units")
    
    pylab.show()

if __name__ == '__main__':
    main()
