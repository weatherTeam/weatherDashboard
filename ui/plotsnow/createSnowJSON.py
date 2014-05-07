import numpy as np
import ogr, osr, os.path, string
#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
#from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads


filename = "snow_weekly.txt"
anomalies = np.genfromtxt(filename, dtype = str, usecols=(1, 2, 3, 4), delimiter = '\t')
#stations = np.genfromtxt("stations.txt", dtype = str)

stationsBase = '{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}'
#outputfile = open('snowcumulation.json', 'a')
lastDate = "nothing"
date = 'not the same'

for anom in anomalies.tolist() :
	
	lat = float(anom[0])
	lon = float(anom[1])
	val = float(anom[3])
	date = str(anom[2])


	if lastDate != date :
		if 'outputfile' in locals() :
			outputfile.write(dumps(stations))
			outputfile.close()

		fileName = "data/snowcumulation-" + date + ".json"
		if os.path.isfile(fileName) :
			outputfile = open(fileName, 'r')
			stations = loads(outputfile.read())
			outputfile.close()
			outputfile = open(fileName, 'w')
		else :
			outputfile = open(fileName, 'w')
			stations = loads(stationsBase)



	#print anom


	#lat = int(lat)
	#lon = int(lon)

	point=ogr.Geometry(ogr.wkbPoint)
	# try:
	# 	point.AddPoint(lon, lat)
	# 	point.Transform(coordTransform)
	# except:
	# 	print "error", e
	# 	continue

	# print "hello"

	# lon = point.GetX()
	# lat = point.GetY()

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



	lastDate = date


outputfile.write(dumps(stations))
outputfile.close()
	