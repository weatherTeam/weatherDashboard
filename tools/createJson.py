from json import dumps, loads
import os

# Author Aubry Cholleton

inputFolder = 'inputData'
#step=0.25
step = 2
for root, directories, files in os.walk(inputFolder):
        for filename in files:
			anomalies = np.genfromtxt(inputFolder+'/'+filename, dtype = str, usecols=(1, 2, 3, 4, 5, 6), delimiter = '\t')

			outputfile = open('day/'+filename+'.json', 'w')

			stats = loads('{"type": "FeatureCollection", "features": []}')

			for anom in anomalies:

				lon = float(anom[1])/1000
				lat = float(anom[0])/1000
				avg = float(anom[2])
				avgMax = float(anom[3])
				avgMin = float(anom[4])
				typeAnom = anom[5]

				stats['features'].append({
					"type": "Feature",
					"geometry": {
						"type": "Polygon",#Point
						"coordinates": [[[lon, lat],[lon, lat+step],[lon+step, lat+step],[lon+step, lat]]]
						#"coordinates": [lon, lat]
					},
					"properties": {
						"avg" : avg,
						"max" : avgMax,
						"min" : avgMin,
						"type" : typeAnom,
					}
				})

			outputfile.write(dumps(stats))
			outputfile.close()
