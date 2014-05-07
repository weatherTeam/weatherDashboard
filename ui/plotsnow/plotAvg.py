import numpy as np
import ogr, osr, os.path, string
import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
#from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads


filename = "snow_average.txt"
values = np.genfromtxt(filename, dtype = str, usecols=(0, 1), delimiter = '\t')
#stations = np.genfromtxt("stations.txt", dtype = str)

stationsBase = '{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}'
#outputfile = open('snowcumulation.json', 'a')

months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]



for m in months :
	allAvg = []
	years = []
	t = []

	for val in values :
		date = str(val[0])
		avg = float(val[1])

	
		splitDate = date.split("-")
		year = splitDate[0]
		month = float(splitDate[1])
	
	
		if month == m :
			years.append(year)
			allAvg.append(avg)
			
	print m
	print month
	print years
	print allAvg
	plt.figure(1)
	#plt.subplot(134)
	plt.plot(years, allAvg, 'bo')

	plt.show()
	
	