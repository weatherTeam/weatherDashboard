import numpy as np
import ogr, osr
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from scipy.interpolate import griddata
#from matplotlib.mlab import griddata
from json import dumps, loads
import os

# Author Aubry Cholleton

stations = np.genfromtxt("stations.txt", dtype = str)
print stations


inputFolder = 'griddedInputData'
for root, directories, files in os.walk(inputFolder):
        for filename in files:
			anomalies = np.genfromtxt(inputFolder+'/'+filename, dtype = str, usecols=(1, 2, 3), delimiter = '\t')

			outputfile = open('griddedDataJson/'+filename+'.json', 'w')

			stats = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}')

			for anom in anomalies:

				lat = float(anom[0])/1000
				lon = float(anom[1])/1000
				val = float(anom[2])

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
