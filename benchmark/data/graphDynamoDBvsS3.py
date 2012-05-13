#!/usr/bin/env python

import csv
import os
import re
import pylab
import matplotlib.ticker as ticker
from matplotlib.pyplot import gca, title, savefig, clf, gcf

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
        if matches is None:
            continue # Skip this file.
        
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
        if isinstance(timestamp, list):
            if record[0] not in timestamp:
                continue
        else:
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
def graphBackendComparison(benchType, pageSize):
    clf()
    
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

    captions = {
        'seqwrite': ('Sequential', 'write'),
        'seqread':  ('Sequential', 'read'),
        'randwrite':('Random', 'write'),
        'randread': ('Random', 'read')
    }
    (order, direction) = captions[benchType]
    
    limits = {
        'seqwrite': [0, 0.3],
        'seqread':  [0, 0.075],
        'randwrite':[0, 0.3],
        'randread': [0, 0.075]
    }
    
    fmt = ticker.FixedFormatter(['S3'] + map(str, dynXData))
    ax = gca()
    ax.get_xaxis().set_major_formatter(fmt)
    ax.set_ylabel(order + " 4K block " + direction + " latency (s)")
    ax.set_xlabel("Provisioned read and write units")
    ax.get_yaxis().grid(color='gray', linestyle='dashed')
    ax.get_yaxis().set_major_locator(ticker.MaxNLocator(10))
    title('Page Size = %dK' % (pageSize / 1024))
    pylab.ylim(limits[benchType])
    dpi = 60
    gcf().dpi = dpi 
    gcf().set_size_inches(400 / dpi, 300 / dpi) 

# Generates a graph comparing performance using different page sizes for
# the same backend.
def graphPageSizeComparison(benchType, backend, writeUnits):
    clf()
    
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
    ax.set_ylabel("Sequential 4K block write latency (s)")
    ax.set_xlabel("Page size (B)")
    ax.get_yaxis().grid(color='gray', linestyle='dashed')
    ax.get_yaxis().set_major_locator(ticker.MaxNLocator(15))
    if backend == 's3':
        title('Backend: S3')
    else:
        title('Backend: DynamoDB; Provisioning Units = %d' % writeUnits)
    
    pylab.show()


# Graphs a comparison of different depths based on a single timestamped
# sample file that was taken on May 13, 2012.
def graphDepthComparison(benchType):
    clf()
    
    data = filter(table,
                  timestamp=range(1336921433, 1336922429+1),
                  benchType=benchType)
    writeUnits = 160
    
    xData = project(data, 'depth')
    yData = project(data, 'latency')
    
    print len(xData)
    print len(yData)
    
    # Each sample represents 5 trials.
    yData = map(lambda x:x/5, yData)
    
    (xData, yData) = condense(xData, yData)
    
    pylab.boxplot(yData)
    fmt = ticker.FixedFormatter(map(str, xData))
    ax = gca()
    ax.get_xaxis().set_major_formatter(fmt)
    ax.set_ylabel("Sequential 4K block write latency (s)")
    ax.set_xlabel("Depth (number of parent directories)")
    ax.get_yaxis().grid(color='gray', linestyle='dashed')
    ax.get_yaxis().set_major_locator(ticker.MaxNLocator(10))
    title('Backend: DynamoDB; Provisioning Units = %d' % writeUnits)
    pylab.ylim([0,0.1])
    
    pylab.show()


# Code to run for this script:
#graphPageSizeComparison('seqwrite', 'dynamodb', 1280)
#for b in BENCH_TYPES:
#    graphBackendComparison(b, 4096)
#    savefig(b + '.png')
graphDepthComparison('seqwrite')
