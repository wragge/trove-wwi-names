import csv
import re
import datetime
import requests
import time
import credentials
import logging

logging.basicConfig(filename='data/errors.txt', level=logging.ERROR,)

class FindNames:
	'''
	Take a CSV data file with names and newspaper citations and
	try to find matching newspaper articles.
	Usage:

	f = FindNames()
	f.find_names()

	'''
	csv_file = 'data/slsa_great_war.csv' # data file location
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