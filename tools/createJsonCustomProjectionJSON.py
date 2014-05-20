import numpy as np
import ogr, osr
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads
import os

stations = np.genfromtxt("stations.txt", dtype = str)
print stations

inputEPSG = 4326
outputEPSG = 4326
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)

outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)

coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

inputFolder = 'inputData'
for root, directories, files in os.walk(inputFolder):
        for filename in files:
			anomalies = np.genfromtxt(inputFolder+'/'+filename, dtype = str, usecols=(1, 2), delimiter = '\t')
			anomaliesCoord = []
			for anomaly in anomalies.tolist() :
				try:
					index = stations[:,0].tolist().index(anomaly[0])
					anomaliesCoord.append([stations[index,1], stations[index,2], anomaly[1]])
				except Exception, e:
					#print "WARNING : ", anomaly[0], "is not a known station, it will not appear on map."
					continue

			outputfile = open('dataJson/'+filename+'.json', 'w')

			stats = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}')

			print len(anomaliesCoord)
			for anom in anomaliesCoord:

				lat = float(anom[0])
				lon = float(anom[1])
				val = float(anom[2])
				if val > 100 or val < -100:
					continue
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

				stats['features'].append({
					"type": "Feature",
					"geometry": {
						"type": "Point",
						"coordinates": [lon, lat]
					},
					"properties": {
						"value" : val,
					}
				})

			outputfile.write(dumps(stats))
			outputfile.close()
				