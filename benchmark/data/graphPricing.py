#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import pylab

N = 3

Ki = 2**10
Mi = 2**20
Gi = 2**30 

# DynamoDB, S3, EBS
read4k =   ( Mi*(0.01/3600)/50,  0.000001*(Gi/(4*Ki))+0.12,   (0.1/1000000)*(Gi/(4*Ki))  )
read16k =  ( Mi*(0.01/3600)/50,  0.000001*(Gi/(16*Ki))+0.12,  (0.1/1000000)*(Gi/(16*Ki)) )
write4k =  ( Mi*(0.01/3600)/10,  0.00001*(Gi/(4*Ki)),         (0.1/1000000)*(Gi/(4*Ki))  )
write16k = ( Mi*(0.01/3600)/10,  0.00001*(Gi/(16*Ki)),        (0.1/1000000)*(Gi/(16*Ki)) )

ind = np.arange(N)  # the x locations for the groups
width = 0.35       # the width of the bars

fig = plt.figure()
ax = fig.add_subplot(111)
rects1 = ax.bar(ind, read16k, width, color='r')
rects2 = ax.bar(ind+width, write16k, width, color='y')

# add some
ax.set_ylabel('Throughput Pricing ($/GiB)')
ax.set_title('Throughput Pricing for 16KiB Pages')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('DynamoDB', 'S3', 'EBS') )

pylab.ylim([0, 3])

ax.legend( (rects1[0], rects2[0]), ('Read', 'Write') )

plt.show()