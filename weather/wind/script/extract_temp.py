#!/usr/local/bin/python

import sys

f = open('tmpTemp', 'w')

with open(sys.argv[1]) as infile:
    for i, x in enumerate(infile):
    	if i >= 1:
    		f.write(x.split()[2] + "," + x.split()[21] +"\n")
f.close()

g = open(sys.argv[2], 'w')

with open('tmpTemp') as infile:
	for line in infile:
		if "*" not in line:
			g.write(line)
g.close()

