import numpy as np
import ogr, osr
#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
#from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads

filename = "snow.txt"
anomalies = np.genfromtxt(filename, dtype = str, usecols=(1, 2, 4), delimiter = '\t')
#stations = np.genfromtxt("stations.txt", dtype = str)

stations = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}')
outputfile = open('snowcumulation.json', 'w')

for anom in anomalies.tolist() :
	

	print anom

	lat = float(anom[0])
	lon = float(anom[1])
	val = float(anom[2])
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
		}
	})

outputfile.write(dumps(stations))
outputfile.close()
	