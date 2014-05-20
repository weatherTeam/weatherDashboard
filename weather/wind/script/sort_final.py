#!/usr/local/bin/python


f = open('part-00000', 'r')
lines = f.readlines()
lines.sort()
f.close()
f = open('part-00000', 'w')
f.writelines(lines)
f.close()
