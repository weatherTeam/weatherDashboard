import urllib2
import re
from random import randint

def get_num_references(title):
	url = "http://toolserver.org/~dispenser/cgi-bin/backlinkscount.py?title="+title
	return urllib2.urlopen(url).read()

def create_country_dictionnary(file_path):
	country_dict = dict()
	for line in open(file_path):
		if line.startswith('#'):
			continue
		line = line.strip()
		line = line.lower()
		demonyms = line.split('\t')[0:2]
		country_dict[demonyms[1]] = demonyms[0]

	return country_dict

def replace_demonym_by_country(string, dict):
	try:
		regex = '\s?(' + '|'.join(dict.keys()) + ')\s?'
		pattern = re.compile(regex)
		result = pattern.sub(lambda x: ' '+dict[x.group(1)]+' ', string)
		return result.strip()
	except Exception as e:
		print e

country_dict = create_country_dictionnary('/Users/quentin/Documents/workspace/EPFL-Workspace/Big Data - WD - Wikipedia/resources/wikipedia/demonyms.txt')

f = open('events.csv', 'w')

for line in open('events.tab'):
	try:
		line = line.strip()
		title, category, start_date, end_date, location = line.split('\t')
		url = title.replace(' ', '_')
		num_ref = get_num_references(url).strip()#randint(0,1000)#0#
		url = "http://en.wikipedia.org/wiki/"+url
		title = "<a href=\""+url+"\">"+title+"</a>"
		location = replace_demonym_by_country(location, country_dict)
		f.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (title, category, start_date, end_date, location, num_ref))
	except Exception as e:
		print line, e

f.close()
