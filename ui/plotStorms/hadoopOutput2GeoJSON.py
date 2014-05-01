from json import dumps, loads
import ogr, osr
import os.path

# https://pypi.python.org/pypi/GDAL/
# http://gis.stackexchange.com/questions/78838/how-to-convert-projected-coordinates-to-lat-lon-using-python

inputEPSG = 4326
outputEPSG = 3857
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)

outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)

coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

inputfile = open('extremeWindUSA2010_500x50.txt')
outputfile = open('extremeWindUSA2010_500x50_EPSG3857.json', 'w')

extremeWinds = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:3857"}}, "features": []}')

for i in range(22): inputfile.next()
counter = 0
for line in inputfile:
    eventid = line[0:11]
    endof_eventid = line[8:11]
    stationid = line[12:23]
    date = line[23:31]
    time = line[31:35]
    lat = line[35:41]
    lon = line[41:47]
    windspeed = line[47:51]
    counter = counter+1
    if lat[:1] != "-" and lat[:1] != "+":
        continue
    if lon[:1] != "-" and lon[:1] != "+":
        continue

    lat = float(lat)/1000
    lon = float(lon)/100

    point=ogr.Geometry(ogr.wkbPoint)
    try:
        point.AddPoint(lon, lat)
        point.Transform(coordTransform)
    except:
        continue

    if endof_eventid == '001':
        geoType = 'Polygon'
    else:
        geoType = 'Point'

    lon = point.GetX()
    lat = point.GetY()

    extremeWinds['features'].append({
        "type": "Feature",
        "geometry": {
            "type": geoType,
            "coordinates": [lon, lat]
        },
        "properties": {
           "eventid" : eventid,
           "stationid" : stationid,
           "date" : date,
           "time" : time,
           "windspeed" : windspeed
        }
    })
    #outputfile.write("%s  LAT:%s LON:%s ELEV:%s" % (name, lat, lon, alt))
print counter
outputfile.write(dumps(extremeWinds))
inputfile.close()
outputfile.close()
