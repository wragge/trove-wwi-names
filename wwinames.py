import csv
import re
import datetime
import requests
import time
import credentials
import logging
import string
from operator import itemgetter

logging.basicConfig(filename='data/errors.txt', level=logging.ERROR,)

class FindNames:
	'''
	Take a CSV data file with names and newspaper citations and
	try to find matching newspaper articles.
	Usage:

	f = FindNames()
	f.find_names()

	'''
	csv_file = 'data/slsa_great_war_2.csv' # data file location
	title_id = '291' # Chronicle - limit searches to this paper
	search_url = 'http://api.trove.nla.gov.au/result'
	item_url = 'http://api.trove.nla.gov.au/newspaper/'

	def find_names(self):
		'''
		Open data file and process a row at a time.
		'''
		with open(self.csv_file, 'rb') as infile:
			reader = csv.reader(infile)
			for row in reader:
				self.process_row(row)

	def process_row(self, row):
		'''
		Construct a Trove newspaper search based on the row contents,
		search for matching results, write matches to file.
		'''
		# Parse values from row
		id = row[0]
		name = row[1].strip('[').strip(']')
		year = row[2]
		# Split multiple references
		references = row[6].split(';')
		names = name.split()
		# Surname should be the last name
		surname = names.pop()
		# Construct query using surname and other names
		query = 'fulltext:"{}" AND ({})'.format(surname, ' OR '.join(names))
		# Get the articles
		articles = self.get_articles(id, year, query)
			#print article['date']
		strong_total = 0
		close_total = 0
		# Loop through the references
		for reference in references:
			strong = []
			close = []
			try:
				# Extract date and page number from reference
				iso_date, pages = self.extract_date(id, reference)
			except TypeError:
				#Couldn't find a valid date and page
				pass
			else:
				# Loop through articles looking for matches on date and page number
				for article in articles:
					if article['date'] == iso_date and str(article['page']) in pages:
						# If both match add to list of 'strong' matches
						strong.append(article)
					elif article['date'] == iso_date:
						# If only date matches, add to list of 'close' matches
						close.append(article)
				if not strong:
					# If there are no strong matches, repeat the search with surname only
					articles = self.get_articles(id, year, 'fulltext:"{}"'.format(surname))
					# Loop through articles looking for matches on date and page number
					for article in articles:
						if article['date'] == iso_date and str(article['page']) in pages:
							# If both match add to list of 'strong' matches
							strong.append(article)
						elif article['date'] == iso_date and not close:
							# If only date matches and there's no other close matches, add to list of 'close' matches
							close.append(article)
				if strong:
					print '{} - Found'.format(id)
				else:
					print '{} - Not found, but {} close.'.format(id, len(close))
				with open('data/slsa_strong.csv', 'ab') as strong_csv:
					strong_writer = csv.writer(strong_csv)
					# Write the 'strong' results out to a csv file
					for article in strong:
						url = 'http://nla.gov.au/nla.news-article{}'.format(article['id'])
						snippet = article['snippet'].replace('<strong>', '').replace('</strong>', '')
						strong_writer.writerow([id, name, reference, article['heading'].encode('utf-8'), article['date'], article['page'], snippet.encode('utf-8'), url])
				with open('data/slsa_close.csv', 'ab') as close_csv:
					close_writer = csv.writer(close_csv)
					# Write the 'close' results out to a csv file
					for article in close:
						url = 'http://nla.gov.au/nla.news-article{}'.format(article['id'])
						snippet = article['snippet'].replace('<strong>', '').replace('</strong>', '')
						close_writer.writerow([id, name, reference, article['heading'].encode('utf-8'), article['date'], article['page'], snippet.encode('utf-8'), url])
				strong_total += len(strong)
				close_total += len(close)
		# Write a summary of the results for this row
		with open('data/slsa_results.csv', 'ab') as results_csv:
			results_writer = csv.writer(results_csv)
			results_writer.writerow([id, name, len(references), strong_total, close_total, strong_total + close_total])
		#print results['response']['zone'][0]['records']['total']
		#result = self.filter_results(results)


	def filter_results(self, results):
		pass 


	def get_articles(self, id, year, query):
		'''
		Retrieve articles from Trove.
		'''
		# Add date to query
		full_query = '{} date:[{} TO {}]'.format(query, year, year)
		harvested = 0
		articles = []
		params = {
			'q': full_query,
			'zone': 'newspaper',
			'key': credentials.TROVE_API_KEY,
			'l-title': self.title_id,
			'n': '100',
			'encoding': 'json',
			's': '0'
		}
		number = params['n']
		# Page through the results set to harvest all results
		while number == params['n']:
			params['s'] = str(harvested)
			results = self.get_results(id, self.search_url, params)
			try:
				number = results['response']['zone'][0]['records']['n']
			except TypeError as e:
				with open('data/errors.txt', 'ab') as errors:
					errors.write('{} - {}\n'.format(id, e))
				break
			else:
				if int(number) > 0:
					articles.extend(results['response']['zone'][0]['records']['article'])
				harvested = harvested + int(number)
			time.sleep(1)
		return articles


	def get_results(self, id, url, params):
		'''
		Get json data.
		'''
		try:
			r = requests.get(url, params=params)
			print r.url
		except requests.exceptions.RequestException as e:
			results = None
			with open('data/errors.txt', 'ab') as errors:
				errors.write('{} - {}\n'.format(id, e))
		else:
			try:
				results = r.json()
			except ValueError as e:
				results = None
				with open('data/errors.txt', 'ab') as errors:
					errors.write('{} - {}\n'.format(id, e))
		return results


	def extract_date(self, id, reference):
		'''
		Extract iso formatted date and page number from text reference.
		'''
		# Get date string
		date_str = re.search(r'(\d{1,2} [a-z,A-Z\,\s]+ \d{4})', reference).group(1)
		# Can be multiple pages in a reference
		pages = re.findall(r'p\. (\d+)', reference)
		# Some normalisation of date strings
		date_str = date_str.replace(',', '').replace('  ', ' ')
		try:
			# Convert to a datetime object
			date_obj = datetime.datetime.strptime(date_str, '%d %B %Y')
		except ValueError as e:
			with open('data/errors.txt', 'ab') as errors:
				errors.write('{} - {} - {}\n'.format(id, e, date_str))
			return None
		else:
			# Return datetime as ISO formatted string
			return (date_obj.strftime('%Y-%m-%d'), pages)

	def get_articles_by_title(self):
		'''
		Use a defined list of title phrases to get a list of articles
		from Trove.
		'''
		title_phrases = [
			'heroes of the great war they gave their lives for king and country',
			'australian soldiers died for their country',
			'casualty list south australia killed in action',
			'on active service', 
			'honoring soldiers', 
			'military honors australians honored', 
			'casualty lists south australian losses list killed in action', 
			'australian soldiers died for his country', 
			'died for their country', 
			'australian soldiers died for the country', 
			'australian soldiers died for their country photographs of soldiers', 
			'quality lists south australian losses list killed in action', 
			'list killed in action', 
			'answered the call enlistments', 
			'gallant south australians how they won their honors', 
			'casualty list south australia died of wounds', 
		]
		articles = []
		for phrase in title_phrases:
			query = 'title:("{}") date:[1914 TO 1919]'.format(phrase)
			harvested = 0
			params = {
				'q': query,
				'zone': 'newspaper',
				'key': credentials.TROVE_API_KEY,
				'l-title': self.title_id,
				'n': '100',
				'encoding': 'json',
				's': '0',
				'sortby': 'dateasc',
				'reclevel': 'full'
			}
			number = params['n']
			# Page through the results set to harvest all results
			while number == params['n']:
				params['s'] = str(harvested)
				results = self.get_results(id, self.search_url, params)
				try:
					number = results['response']['zone'][0]['records']['n']
				except TypeError as e:
					with open('data/title_errors.txt', 'ab') as errors:
						errors.write('{} - {}\n'.format(id, e))
					break
				else:
					if int(number) > 0:
						articles.extend(results['response']['zone'][0]['records']['article'])
					harvested = harvested + int(number)
				time.sleep(1)
		# Remove duplicates
		articles = { article['id']:article for article in articles }.values()
		# Sort by article title
		articles = sorted(articles, key=itemgetter('heading'))
		with open('data/articles.csv', 'ab') as titles_csv:
			for article in articles:
				titles_writer = csv.writer(titles_csv)
				titles_writer.writerow([
						article['id'],
						article['heading'].encode('utf-8'),
						article['title']['value'],
						article['date'],
						article['page'],
						article['identifier'],
						article['illustrated'],
						article['wordCount'],
						article['correctionCount']
					])

	def get_title_groups(self, limit=10):
		'''
		Try to extract common titles/phrases from a list of articles.
		'''
		titles = {}
		articles_file = 'data/slsa_strong.csv'
		with open(articles_file, 'rb') as infile:
			reader = csv.reader(infile)
			reader.next()
			for row in reader:
				title = row[3]
				# Normalise titles
				title = title.strip().lower()
				# Remove punctuation
				title = title.translate(string.maketrans("",""), string.punctuation)
				# Remove multiple spaces
				title = re.sub(r'\s+', ' ', title)
				# Remove ordinals - 422nd etc
				title = re.sub(r'\d+(rd|st|th|nd) ', '', title)
				title = re.sub(r'no \d+ ', '', title)
				try:
					titles[title] += 1
				except KeyError:
					titles[title] = 1
 		#titles = sorted(titles.items(), key=itemgetter(1), reverse=True)
 		phrases = [title for title, count in titles if count >= limit]
		print phrases
		#for t in titles:
		#	print '{}: {}'.format(t[0], t[1])			

