import ogr, osr

# Author Aubry Cholleton

# https://pypi.python.org/pypi/GDAL/
# http://gis.stackexchange.com/questions/78838/how-to-convert-projected-coordinates-to-lat-lon-using-python

inputEPSG = 4326
outputEPSG = 4326#3857
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)

outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)

coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

inputfile = open('ish-history.txt')
outputfile = open('stations.txt', 'w')

for i in range(22): inputfile.next()
counter = 0
for line in inputfile:
	id = line[0:6]
	name = line[13:43].strip()
	lat = line[58:64]
	lon = line[65:72]
	alt = line[73:79]
	ctry = line[43:48]

	if ctry == "SW SZ" or 1:
	#if ctry == "US US":
		if id == "999999":
			continue
		if lat[:1] != "-" and lat[:1] != "+":
			continue
		if lon[:1] != "-" and lon[:1] != "+":
			continue
		if alt[:1] != "-" and alt[:1] != "+":
			continue
		lat = float(lat)/1000
		lon = float(lon)/1000
		alt = float(alt)/10
		if lat == -99.999 or lon == -999.999 or lat == 0 or lon == 0:
			continue
		#if lon>-62.629 or lon<-127.8955 or lat<22.490 or lat>51.109:
		#	continue




		#lat = int(lat)
		#lon = int(lon)

		point=ogr.Geometry(ogr.wkbPoint)
		try:
			point.AddPoint(lon, lat)
			point.Transform(coordTransform)
		except:
			continue

		counter = counter+1

		lon = point.GetX()
		lat = point.GetY()

		outputfile.write("%s %s %s\n" % (id, lat, lon))
print counter, "weather stations successfully exported"
inputfile.close()
outputfile.close()
