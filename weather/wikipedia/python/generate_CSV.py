import argparse
from time import strptime
from time import gmtime



def inputDateFormat(string):
	try:
		return strptime(string,"%d/%m/%Y")
	except ValueError as err:
	   	msg = "%s is not a valid date (%s)" % (string,err)
	   	raise argparse.ArgumentTypeError(msg)

def add_to_CSV_of_month(month, year, line):
	str_year = str(year)
	str_month = str(month)

	if year >= 1975 and year <= 2014 :
		with open('wiki-csv/'+str_year+'-'+str_month+'.tab', 'a') as month_csv:
			month_csv.write(line)

def get_num_references(title):
	url = "http://toolserver.org/~dispenser/cgi-bin/backlinkscount.py?title="+title
	return urllib2.urlopen(url).read()


	
def main():

	for line in open('events.tab'):
		line = line.strip()
		title, category, start_date, end_date, location = line.split('\t')

		url = title.replace(' ', '_')
		num_ref = get_num_references(url).strip()
		url = "http://en.wikipedia.org/wiki/"+url
		title = "<a href=\""+url+"\">"+title+"</a>"
		line = ("%s\t%s\t%s\t%s\t%s\t%s\n" % (title, category, start_date, end_date, location,num_ref))

		try:
			start = strptime(start_date,"%d/%m/%Y")
			end = strptime(end_date,"%d/%m/%Y")

			if start > end: #sometimes, articles are buggy and have a start date after the end date, which causes an infinite loop below
				continue

			if(start.tm_year == end.tm_year and start.tm_mon == end.tm_mon):
				add_to_CSV_of_month(start.tm_mon, start.tm_year, line)
			
			else:
				current_month = start.tm_mon
				current_year = start.tm_year

				while (current_month != end.tm_mon or current_year != end.tm_year):
					add_to_CSV_of_month(current_month, current_year, line)
					current_month = current_month % 12 + 1 
					if current_month == 1:
						current_year += 1

				add_to_CSV_of_month(end.tm_mon, end.tm_year, line)

		except ValueError as err:
			pass

main()




