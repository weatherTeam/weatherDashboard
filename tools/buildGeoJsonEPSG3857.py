from json import dumps, loads
import ogr, osr
# Author Aubry Cholleton

# https://pypi.python.org/pypi/GDAL/
# http://gis.stackexchange.com/questions/78838/how-to-convert-projected-coordinates-to-lat-lon-using-python

inputEPSG = 4326
outputEPSG = 3857
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)

outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)

coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

inputfile = open('ish-history.txt')
outputfile = open('stations.json', 'w')

stations = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:3857"}}, "features": []}')

for i in range(22): inputfile.next()
counter = 0
for line in inputfile:
	name = line[13:43].strip()
	lat = line[58:64]
	lon = line[65:72]
	alt = line[73:79]
	ctry = line[43:48]
	counter = counter+1
	#if ctry == "SW SZ":
	if lat[:1] != "-" and lat[:1] != "+":
		continue
	if lon[:1] != "-" and lon[:1] != "+":
		continue
	if alt[:1] != "-" and alt[:1] != "+":
		continue
	lat = float(lat)/1000
	lon = float(lon)/1000
	alt = float(alt)/10
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
		#"properties": {
		#	"name" : name
		#   "elevation" : alt,
		#}
	})
	#outputfile.write("%s  LAT:%s LON:%s ELEV:%s" % (name, lat, lon, alt))
print counter
outputfile.write(dumps(stations))
inputfile.close()
outputfile.close()
