#!/usr/local/bin/python

with open ('part-00000', 'r') as infile:
	
	old_date = infile.readline().split()[0]

	for x in infile.readlines():
		
		date = x.split()[0]
		
		if (date != old_date):
			old_date = date
		
		out = open("months/%s" %old_date, 'a')
		out.write(x)
		out.close()

	infile.close()
