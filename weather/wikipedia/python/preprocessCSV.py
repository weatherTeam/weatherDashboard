f = open('events.csv', 'w+')

for line in open('events.tab'):
	title, category, start_date, end_date, location = line.split('\t')
	if not title.startswith("Category:") and not title.startswith("Template:") and not title.startswith("File:") and not title.startswith("Book:") and not title.startswith("Wikipedia:"):
		url = "http://en.wikipedia.org/wiki/"+title
		url = url.replace(' ', '_')
		title = "<a href=\""+url+"\">"+title+"</a>"

		f.write("%s\t%s\t%s\t%s\t%s" % (title, category, start_date, end_date, location))

f.close()
