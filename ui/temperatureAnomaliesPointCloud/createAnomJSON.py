import numpy as np
import ogr, osr
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads

filename = "082011"
anomalies = np.genfromtxt(filename, dtype = str, usecols=(1, 2), delimiter = '\t')
stations = np.genfromtxt("stations.txt", dtype = str)


anomaliesCoord = []
for anomaly in anomalies.tolist() :
	try:
		index = stations[:,0].tolist().index(anomaly[0])
		anomaliesCoord.append([stations[index,1], stations[index,2], anomaly[1]])
	except Exception, e:
		print "WARNING : ", anomaly[0], "is not a known station, it will not appear on map."
		continue

inputEPSG = 4326
outputEPSG = 4326
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)

outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)

coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

outputfile = open('anom.json', 'w')

stations = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}')

for anom in anomaliesCoord:
	lat = float(anom[0])
	lon = float(anom[1])
	val = float(anom[2])
	#lat = int(lat)
	#lon = int(lon)

	point=ogr.Geometry(ogr.wkbPoint)
	try:
		point.AddPoint(lon, lat)
		point.Transform(coordTransform)
	except:
		continue

	lon = point.GetX()
	lat = point.GetY()

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
	