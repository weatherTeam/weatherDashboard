import numpy as np
import ogr, osr, os.path, string
#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
#from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads
from sets import Set


filename = "snow_monthly.txt"
anomalies = np.genfromtxt(filename, dtype = str, usecols=(1, 2, 3, 4), delimiter = '\t')
#stations = np.genfromtxt("stations.txt", dtype = str)

stationsBase = '{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}'
#outputfile = open('snowcumulation.json', 'a')
lastDate = "nothing"
date = 'not the same'

i = 0;
dates = Set()


#bad performance, but didn't find any quick and easy wait to sort the array. But at
#least it didn't close/open/close/open files which was of course much worse.
for anom in anomalies.tolist() :
	date = str(anom[2])
	dates.add(date)
	
for currentDate in dates :
	print currentDate
	
	fileName = "data_monthly/snowcumulation-" + currentDate + ".json"
	outputfile = open(fileName, 'w')
	stations = loads(stationsBase)

	for anom in anomalies.tolist() :

		date = str(anom[2])
		if currentDate == date :
			lat = float(anom[0])
			lon = float(anom[1])
			val = float(anom[3])
			if val > 0.0 :
				stations['features'].append({
					"type": "Feature",
					"geometry": {
						"type": "Point",
						"coordinates": [lon, lat]
					},
					"properties": {
						"value" : val,
						"time" : date
					}
				})

	outputfile.write(dumps(stations))
	outputfile.close()






	