import csv
import sys
from graphLocal import getAverages
import numpy as np
from matplotlib import pyplot as plt

__author__ = 'jpaton'

def printUsage():
    print "Usage: %s <csv file name for raw bdb> <csv file name for dynamoFS> <filename for throughput> <filename for speedup>"

def plotThroughput(data, filename, legendStrings):
    ind = np.arange(20) + 0.1
    fig = plt.figure(figsize = (8, 4), dpi = 600)
    ax = fig.add_subplot(111)
    barwidth = 0.8
    rects = list()
#    colors = ['r', 'g', 'y', 'b']
    xlabels = map(lambda x: float(x[0]) / 10**6, data)
#    for i in range(len(data)):
#        toPlot = map(lambda x: float(x[0]) / 10**6 / float(x[1]) , data[i])
#        rects.append(ax.bar(
#            ind + i * barwidth,
#            toPlot,
#            width = barwidth,
#            color = colors[i % len(colors)],
#        ))

    rect = ax.bar(
        ind,
        [i[0] / 10**6 / i[1] for i in data],
        width = barwidth,
        color = 'b',
    )

    ax.set_xticks((ind + barwidth / 2)[1::2])
    ax.set_xticklabels(xlabels[1::2])
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_xlabel("File size (MB)")
    ax.legend([rect], legendStrings, loc="best", prop={'size':6})
    plt.savefig(filename)

def plotSpeedup(data_raw, data_dfs, filename, legendStrings):
    ind = np.arange(20) + 0.1
    fig = plt.figure(figsize = (8, 4), dpi = 600)
    ax = fig.add_subplot(111)
    barwidth = 0.8
    xlabels = map(lambda x: float(x[0]) / 10**6, data_raw)

    data_raw = [i[0] / i[1] for i in data_raw]
    data_dfs = [i[0] / i[1] for i in data_dfs]

    rect = ax.bar(
        ind,
        [data_dfs[i] / data_raw[i] for i in range(data_raw)],
        width = barwidth,
        color = 'b',
    )

    ax.set_xticks((ind + barwidth / 2)[1::2])
    ax.set_xticklabels(xlabels[1::2])
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_xlabel("File size (MB)")
    ax.legend([rect], legendStrings, loc="best", prop={'size':6})
    plt.savefig(filename)

def main():
    if len(sys.argv) != 5:
        printUsage()
        sys.exit(1)

    raw_filename = sys.argv[1]
    raw_fieldnames = [
        "benchtype",
        "fstype",
        "size",
        "time",
    ]
    reader = csv.DictReader(open(raw_filename, 'r'), fieldnames = raw_fieldnames)
    data_raw = [row for row in reader]
    data_raw = [(float(size), float(time)) for (size, time) in getAverages(data_raw).items()]

    dfs_filename = sys.argv[3]
    raw_fieldnames = [
        "benchtype",
        "fstype",
        "size",
        "time",
    ]
    reader = csv.DictReader(open(dfs_filename, 'r'), fieldnames = dfs_fieldnames)
    data_dfs = [row for row in reader]
    data_dfs = [(float(size), float(time)) for (size, time) in getAverages(data_dfs).items()]

    plotThroughput(sorted(data_raw), sys.argv[2], ["Write"])
    plotSpeedup(data_raw, data_dfs, sys.argv[4], ["Write"])

if __name__ == '__main__':
    main()