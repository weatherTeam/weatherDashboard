from json import dumps, loads
import ogr, osr
import os.path
import sys

inputfilename = str(sys.argv[1])
outputfilename = str(sys.argv[2]) #must be .json

inputfile = open(inputfilename)
outputfile = open(outputfilename, 'w')

extremeWinds = loads('{"type": "FeatureCollection","crs": {"type": "name","properties": {"name": "EPSG:4326"}}, "features": []}')

counter = 0
for line in inputfile:
    startdate = line[7:19]
    enddate = line[19:31]
    lat = line[31:38]
    lon = line[38:45]
    radius = line[45:51]
    windspeed = line[51:54]
    rainfall = line[54:57]
    counter = counter+1
    if lat[:1] != "-" and lat[:1] != "+":
        continue
    if lon[:1] != "-" and lon[:1] != "+":
        continue

    lat = float(lat)/1000
    lon = float(lon)/1000
    windspeed = (float(windspeed)/10)*3.6
    rainfall = float(rainfall)
    radius = float(radius)

    if windspeed >= 100:
        extremeWinds['features'].append({
            "type": "Feature",
            "geometry": {
                "type": 'Point',
                "coordinates": [lon, lat]
            },
            "properties": {
               "startdate" : startdate,
               "enddate" : enddate,
               "radius" : radius,
               "rainfall" : rainfall,
               "windspeed" : windspeed
             }
        })
print counter
outputfile.write(dumps(extremeWinds))
inputfile.close()
outputfile.close()
