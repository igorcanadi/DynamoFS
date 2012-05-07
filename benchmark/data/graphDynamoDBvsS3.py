#!/usr/bin/env python

import csv
import os
import re
import pylab
import matplotlib.ticker as ticker
from matplotlib.pyplot import gca, title

BENCH_TYPES = ['seqwrite', 'seqread', 'randwrite', 'randread'];

# We rad all the data into a table with the following columns.
COLUMNS = ['timestamp', 'benchType', 'backend', 'depth', 'fileSize',
           'readUnits', 'writeUnits', 'latency', 'pageSize'];

# Read data into the table.
table = []
homes = ['dynamodb-ec2', 's3-ec2'] # Load data from two directories.
csvFields = ["benchType", "backend", "depth", "fileSize", "latency"]

pageSizePattern = re.compile(r"page(\d+)")
dynPattern = re.compile(r"(\d+)-r(\d+)-w(\d+).csv")
s3Pattern = re.compile(r"(\d+).csv")

for home in homes:
    # Inside each home are folders for different page sizes.
    for p in os.listdir(home):
        matches = pageSizePattern.match(p)
        pageSize = int(matches.group(1))        
    
        for f in os.listdir(home + '/' + p):
            matches = dynPattern.match(f)
            if matches is not None:
                benchType = 'dynamodb'
            else:
                matches = s3Pattern.match(f)
                if matches is not None:
                    benchType = 's3'
                else:
                    benchType = None
            
            if benchType is not None:
                timestamp = int(matches.group(1))
                
                if benchType == 'dynamodb':
                    readUnits = int(matches.group(2))
                    writeUnits = int(matches.group(3))
                else:
                    # Put in dummy values for S3, since S3 is not provisioned.
                    readUnits = 0
                    writeUnits = 0
                
                csvReader = csv.DictReader(open(home + '/' + p + '/' + f, 'r'), csvFields)
                for csvLine in csvReader:
                    # Build a record for the table.
                    record = [timestamp,
                              csvLine['benchType'],
                              csvLine['backend'],
                              int(csvLine['depth']),
                              int(csvLine['fileSize']),
                              readUnits,
                              writeUnits,
                              float(csvLine['latency']),
                              pageSize
                              ]
                    table.append(record)


# Filters the data, producing an arbitrary subset of records.
def filter(table,
           timestamp=None,
           benchType=None,
           readUnits=None,
           writeUnits=None,
           backend=None,
           pageSize=None):
    measurements = []
    for record in table:
        if timestamp is not None and timestamp != record[0]:
            continue
        if benchType is not None and benchType != record[1]:
            continue
        if backend is not None and backend != record[2]:
            continue
        if readUnits is not None and readUnits != record[5]:
            continue
        if writeUnits is not None and writeUnits != record[6]:
            continue
        if pageSize is not None and pageSize != record[8]:
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
            newXCoords.append(lastX)
            yList = [y]
            lastX = x
            
    # Clean up the from the very last loop iteration.
    newYCoords.append(yList)
    newXCoords.append(lastX)
            
    return (newXCoords, newYCoords)
    

# Generates a graph comparing different backends (s3 and dynamodb) and
# different provisioning levels (for dynamodb). THe page size to use for
# comparison is user-supplied.
def graphBackendComparison(pageSize):
    # Do analysis using random write numbers.
    benchType = 'seqwrite'
    
    dynData = filter(table,
                     benchType=benchType,
                     backend='dynamodb',
                     pageSize=pageSize)
    s3Data = filter(table,
                    benchType=benchType,
                    backend='s3',
                    pageSize=pageSize)
    
    dynXData = project(dynData, 'writeUnits')
    dynYData = project(dynData, 'latency')
    s3YData = project(s3Data, 'latency')
    
    # Each sample represents 10 trials.
    dynYData = map(lambda x:x/10, dynYData)
    s3YData = map(lambda x:x/10, s3YData)
    
    (dynXData, dynYData) = condense(dynXData, dynYData)
    
    # Merge the dynamodb and s3 datasets to they can be plotted in the same axes.
    yData = [s3YData] + dynYData
    pylab.boxplot(yData)
    
    fmt = ticker.FixedFormatter(['S3'] + map(str, dynXData))
    ax = gca()
    ax.get_xaxis().set_major_formatter(fmt)
    ax.set_ylabel("Sequential block write latency (s)")
    ax.set_xlabel("Provisioned write units")
    ax.get_yaxis().grid(color='gray', linestyle='dashed')
    ax.get_yaxis().set_major_locator(ticker.MaxNLocator(15))
    title('Page Size = %d' % pageSize)
    
    pylab.show()

# Generates a graph comparing performance using different page sizes for
# the same backend.
def graphPageSizeComparison(backend, writeUnits):
    # Do analysis using random write numbers.
    benchType = 'seqwrite'
    
    data = filter(table,
                  benchType=benchType,
                  backend=backend,
                  writeUnits=writeUnits)
    
    xData = project(data, 'pageSize')
    yData = project(data, 'latency')
    
    # Each sample represents 10 trials.
    yData = map(lambda x:x/10, yData)
    
    (xData, yData) = condense(xData, yData)
    
    pylab.boxplot(yData)
    fmt = ticker.FixedFormatter(map(str, xData))
    ax = gca()
    ax.get_xaxis().set_major_formatter(fmt)
    ax.set_ylabel("Sequential block write latency (s)")
    ax.set_xlabel("Page size (B)")
    ax.get_yaxis().grid(color='gray', linestyle='dashed')
    ax.get_yaxis().set_major_locator(ticker.MaxNLocator(15))
    if backend == 's3':
        title('Backend: S3')
    else:
        title('Backend: DynamoDB; Write Units = %d' % writeUnits)
    
    pylab.show()


# Code to run for this script:
#graphPageSizeComparison('s3', 0)
graphBackendComparison(4096)
