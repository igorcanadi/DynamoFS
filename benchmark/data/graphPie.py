#!/usr/bin/env python
__author__ = 'jpaton'

from matplotlib import pyplot as plt
import numpy as np

s3_total = 14.881
s3_data = {
    'get': 6.794,
    'put': 6.104,
    'construct' : 1.065,
}
s3_data['other'] = s3_total - sum(s3_data.values())

dbd_total = 9.047
dbd_data = {
    'get': 5.439,
    'put': 2.545,
    'construct' : 0.120,
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
    rects.append((ax.bar(ind + offset, others, width, color='y'), "Other"))
    rects.append((ax.bar(ind + offset, constructs, width, color='g', bottom = others), "Constructors"))
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

    plt.savefig("stacked.png")

if __name__ == '__main__':
    main()
