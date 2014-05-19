import numpy as np
import ogr, osr, os.path, string
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from json import dumps, loads
from matplotlib.pyplot import *


filename = "snow_average.txt"
values = np.genfromtxt(filename, dtype = str, usecols=(0, 1), delimiter = '\t')
values = np.sort(values, 0)
#stations = np.genfromtxt("stations.txt", dtype = str)

stationsBase = '{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}'
#outputfile = open('snowcumulation.json', 'a')

months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

colors = cm.rainbow(np.linspace(0, 1, 12))

all = []
for m in months :
	allAvg = []
	years = []
	t = []
	
	for val in values :
		date = str(val[0])
		splitDate = date.split("-")
		year = splitDate[0]
		
	

	for val in values :
		date = str(val[0])
		avg = float(val[1])

	
		splitDate = date.split("-")
		year = splitDate[0]
		month = float(splitDate[1])
	
	
		if month == m :
			years.append(year)
			allAvg.append(avg)
			all.append(allAvg)
			
	print m
	print month
	print years
	print allAvg
	plt.figure(1)
	#plt.subplot(134)
	plt.plot(years, allAvg, 'r-', color=colors[m-1], label=m)
	#handles, labels = ax.get_legend_handles_labels()
	#legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
	

plt.show()
	
	