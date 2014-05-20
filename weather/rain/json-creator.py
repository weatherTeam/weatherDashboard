from json import dumps, loads
import os


        
inputFolder = 'day'
for root, directories, files in os.walk(inputFolder):
    for filename in files:
        inputfile = open(inputFolder+'/'+filename,'r')
        filename = filename[0:-4]
        outputfile = open(inputFolder+'-json/'+filename+'.json', 'w+')
        stats = loads('{"type": "FeatureCollection", "features": []}')

        for line in inputfile:
            splitted = line.split('\t')
            ID = splitted[1]
            latitude = splitted[2].split(':')[1]
            longitude = splitted[3].split(':')[1]
            name = splitted[4].split(':')[1]
            state = splitted[5].split(':')[1]
            date = splitted[6].split(':')[1]
            deviation = splitted[7].split(':')[1]
            value = splitted[8].split(':')[1]
            avg = splitted[9].split(':')[1]

            stats['features'].append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [longitude,latitude]},
                "properties": {
                    "state" : state,
                    "name" : name,
                    "date" : date,
                    "deviation" : deviation,
                    "value" : value,
                }
            })
        
        outputfile.write(dumps(stats))
        outputfile.close()
        inputfile.close()