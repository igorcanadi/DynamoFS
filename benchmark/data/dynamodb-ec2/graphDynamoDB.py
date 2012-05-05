#!/usr/bin/env python

import sys
import numpy
import csv
import os
import re
import pylab

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
            break
        if benchType is not None and benchType != record[1]:
            break
        if readUnits is not None and readUnits != record[5]:
            break
        if writeUnits is not None and writeUnits != record[6]:
            break
        measurements.append(record)
    return measurements

# Trims columns out of a table of data.
def project(table, columns):
    # Convert column names to indices.
    columns = map(COLUMNS.index, columns)

    newTable = []
    for record in table:
        newRecord = []
        for column in columns:
            newRecord.append(record[column])
        newTable.append(newRecord)
    return newTable
        
    
def mean(data):
    return sum(data) / len(data)

def main():
    seqwriteData = filter(table, benchType='seqwrite')
    xData = project(seqwriteData, ['writeUnits'])
    yData = project(seqwriteData, ['latency'])
     
    print xData
    print yData
    pylab.scatter(xData, yData)
    pylab.show()

if __name__ == '__main__':
    main()
