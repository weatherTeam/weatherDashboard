import argparse
from time import strptime
from time import gmtime
import urllib2
import json



def inputDateFormat(string):
	try:
		return strptime(string,"%d/%m/%Y")
	except ValueError as err:
	   	msg = "%s is not a valid date (%s)" % (string,err)
	   	raise argparse.ArgumentTypeError(msg)

def gps_to_places(latitude, longitude):

	url = "http://nominatim.openstreetmap.org/reverse?format=json&lon=%f&lat=%f&addressdetails=1&accept-language=en&email=alpsweather@groupes.epfl.ch"%(longitude, latitude)
	address = json.load(urllib2.urlopen(url))

	if 'error' in address:
		raise Exception("Unable to locate the station at https://www.google.com/maps/place/%f,%f"%(latitude, longitude))
	else:
		return address['address']['state'], address['address']['country']

def load_demonyms(file):
	for line in open(file):
		line.split(' ')
	
	

def main():
	parser = argparse.ArgumentParser(description='Search for weather events corresponding to criteria')
	parser.add_argument('-d', action="store", dest='event_date',type=inputDateFormat, help = 'Event date format: dd/mm/yyyy')
	parser.add_argument('-l', action="store", dest='event_location', help='Event location')
	parser.add_argument('-g', action="store", dest='event_gps', help='Event location')
	parser.add_argument('-t', action="store", dest='event_category', help = 'Event category (storm, cyclone, blizzard, etc.)')
	parser.add_argument('-f', action="store", dest='event_file', default='events.tsv', help = 'Event file (default: %(defaut)s)')

	args = parser.parse_args()
	event_date = args.event_date
	event_location = args.event_location
	event_gps = args.event_gps
	event_category = args.event_category
	event_file = args.event_file

	for line in open(event_file):
		title, category, start_date, end_date, location = line.split('\t')
		#TODO start < date < end

		if start_date != "":
			start = strptime(start_date,"%d/%m/%Y")
		else:
			#before first day of data set
			start = strptime("31/12/1899","%d/%m/%Y")

		if end_date != "":
			end = strptime(end_date,"%d/%m/%Y")
		else:
			#today
			end = gmtime()

		if (event_location == None or event_location in location) and (event_date == None or start <= event_date <= end) :
			print title
try:
	state, country = gps_to_places(+47.083, +006.800)
except Exception as e:
	print e