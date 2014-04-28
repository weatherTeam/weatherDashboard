import numpy as np
import ogr, osr
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
#from scipy.interpolate import griddata
from matplotlib.mlab import griddata

filename = "072005"
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
print anomaliesCoord

# Code from https://gist.github.com/davydany/3789221
 # 'a' is of the format [(lats, lons, data), (lats, lons, data)... (lats, lons, data)]
lats = [ float(x[0]) for x in anomaliesCoord ]
lons = [ float(x[1]) for x in anomaliesCoord ]
data = [ float(x[2]) for x in anomaliesCoord ]
print lats
print data
lat_min = min(lats)
lat_max = max(lats)
lon_min = min(lons)
lon_max = max(lons)

inputEPSG = 4326
outputEPSG = 3857
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(inputEPSG)
outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(outputEPSG)
coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
point=ogr.Geometry(ogr.wkbPoint)
point.AddPoint(lon_min, lat_min)
point.Transform(coordTransform)
lon = point.GetX()
lat = point.GetY()
print lon, lat

point=ogr.Geometry(ogr.wkbPoint)
point.AddPoint(lon_max, lat_max)
point.Transform(coordTransform)
lon = point.GetX()
lat = point.GetY()
print lon, lat


data_min = min(data)
data_max = max(data)
spatial_resolution = 0.06
fig = plt.figure()
x = np.array(lats)
y = np.array(lons)
z = np.array(data)
xinum = (lat_max - lat_min) / spatial_resolution
yinum = (lon_max - lon_min) / spatial_resolution
xi = np.linspace(lat_min, lat_max + spatial_resolution, xinum) # same as [lat_min:spatial_resolution:lat_max] in matlab
yi = np.linspace(lon_min, lon_max + spatial_resolution, yinum) # same as [lon_min:spatial_resolution:lon_max] in matlab
xi, yi = np.meshgrid(xi, yi)
zi = griddata(x, y, z, xi, yi)
fig = plt.figure(frameon=False)
ax = fig.add_axes([0, 0, 1, 1])
ax.axis('off')
ax.margins(0)

m = Basemap(epsg = '3854',llcrnrlat=lat_min, urcrnrlat=lat_max,llcrnrlon=lon_min, urcrnrlon=lon_max, resolution='l', area_thresh=10000)
m.drawcoastlines()
m.drawstates()
m.drawcountries()
lat, lon = m.makegrid(zi.shape[1], zi.shape[0])
x,y = m(lat, lon)
#a,b = m(lat_min, lat_max)
#c,d = m(lon_min, lon_max)
#print a,b,c,d

m.contourf(x, y, zi, levels = np.arange(-50,50,2), extend='both')

plt.subplots_adjust(top=0.1, bottom=0)
#plt.show()


#fig.savefig('test.png')
with open('test.png', 'w') as outfile:
    fig.canvas.print_png(outfile)


	