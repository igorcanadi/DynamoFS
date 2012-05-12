#!/usr/bin/env python
__author__ = 'jpaton'

from matplotlib import pyplot as plt
import numpy as np
import pylab

s3_total = 18.885
s3_data = {
    'get': 9.200,
    'put': 5.906,
    'construct' : 2.952
}
s3_data['other'] = s3_total - sum(s3_data.values())

dbd_total = 13.257
dbd_data = {
    'get': 6.732,
    'put': 5.592,
    'construct' : 0.116
}
dbd_data['other'] = dbd_total - sum(dbd_data.values())

def main():
    print s3_data
    print dbd_data

    gets = [s3_data['get'], dbd_data['get']]
    puts = [s3_data['put'], dbd_data['put']]
    others = [s3_data['other'], dbd_data['other']]
    constructs = [s3_data['construct'], dbd_data['construct']]

    fig = plt.figure(figsize = (6, 6), dpi = 600)
    ax = fig.add_subplot(111)
    ind = np.arange(2)
    width = 0.35
    offset = 0.13
    rects = list()
    rects.append((ax.bar(ind + offset, others, width, color='y'), "Python overheads"))
    rects.append((ax.bar(ind + offset, constructs, width, color='g', bottom = others), "Connection setup"))
    rects.append((ax.bar(ind + offset, gets, width, color='r', bottom = map(sum, zip(others, constructs))), "Gets"))
    rects.append((ax.bar(ind + offset, puts, width, color='b', bottom = map(sum, zip(others, constructs, gets))), "Puts"))
    ax.set_xticklabels(['S3', 'DynamoDB'])
    ax.set_xticks(ind + width / 2 + offset)
    ax.set_ylabel("Time (s)")
    ax.set_xlabel("Backend")

#    ax.legend([p1, p2, p3, p4], ['Get', 'Put', 'Other', 'Constructors'], loc="best")
    rects, labels = zip(*reversed(rects))
    print labels
    ax.legend(rects, labels)

    dpi = 60
    plt.gcf().dpi = dpi
    plt.gcf().set_size_inches(300 / dpi, 300 / dpi)
    pylab.ylim([0, 22])

    plt.savefig("stacked.png")

if __name__ == '__main__':
    main()
