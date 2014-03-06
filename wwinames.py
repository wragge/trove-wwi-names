import csv
import re
import datetime
import requests
import time
import credentials
import logging

logging.basicConfig(filename='data/errors.txt', level=logging.ERROR,)

class FindNames:

	csv_file = 'data/slsa_great_war.csv'
	title_id = '291' #Chronicle
	search_url = 'http://api.trove.nla.gov.au/result'
	item_url = 'http://api.trove.nla.gov.au/newspaper/'

	def find_names(self):
		with open(self.csv_file, 'rb') as infile:
			reader = csv.reader(infile)
			for row in reader:
				self.process_row(row)

	def process_row(self, row):
		id = row[0]
		name = row[1].strip('[').strip(']')
		year = row[2]
		references = row[6].split(';')
		names = name.split()
		surname = names.pop()
		query = 'fulltext:"{}" AND ({})'.format(surname, ' OR '.join(names))
		articles = self.get_articles(id, year, query)
			#print article['date']
		strong_total = 0
		close_total = 0
		for reference in references:
			strong = []
			close = []
			try:
				iso_date, pages = self.extract_date(id, reference)
			except TypeError:
				#Couldn't find a valid date and page
				pass
			else:
				for article in articles:
					if article['date'] == iso_date and str(article['page']) in pages:
						strong.append(article)
					elif article['date'] == iso_date:
						close.append(article)
				if not strong:
					articles = self.get_articles(id, year, 'fulltext:"{}"'.format(surname))
					for article in articles:
						if article['date'] == iso_date and str(article['page']) in pages:
							strong.append(article)
						elif article['date'] == iso_date and not close:
							close.append(article)
				if strong:
					print '{} - Found'.format(id)
				else:
					print '{} - Not found, but {} close.'.format(id, len(close))
				with open('data/slsa_strong.csv', 'ab') as strong_csv:
					strong_writer = csv.writer(strong_csv)
					for article in strong:
						url = 'http://nla.gov.au/nla.news-article{}'.format(article['id'])
						snippet = article['snippet'].replace('<strong>', '').replace('</strong>', '')
						strong_writer.writerow([id, name, reference, article['heading'].encode('utf-8'), article['date'], article['page'], snippet.encode('utf-8'), url])
				with open('data/slsa_close.csv', 'ab') as close_csv:
					close_writer = csv.writer(close_csv)
					for article in close:
						url = 'http://nla.gov.au/nla.news-article{}'.format(article['id'])
						snippet = article['snippet'].replace('<strong>', '').replace('</strong>', '')
						close_writer.writerow([id, name, reference, article['heading'].encode('utf-8'), article['date'], article['page'], snippet.encode('utf-8'), url])
				strong_total += len(strong)
				close_total += len(close)
		with open('data/slsa_results.csv', 'ab') as results_csv:
			results_writer = csv.writer(results_csv)
			results_writer.writerow([id, name, len(references), strong_total, close_total, strong_total + close_total])
		#print results['response']['zone'][0]['records']['total']
		#result = self.filter_results(results)


	def filter_results(self, results):
		pass 


	def get_articles(self, id, year, query):
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
		date_str = re.search(r'(\d{1,2} [a-z,A-Z\,\s]+ \d{4})', reference).group(1)
		pages = re.findall(r'p\. (\d+)', reference)
		#print pages
		date_str = date_str.replace(',', '').replace('  ', ' ')
		try:
			date_obj = datetime.datetime.strptime(date_str, '%d %B %Y')
		except ValueError as e:
			with open('data/errors.txt', 'ab') as errors:
				errors.write('{} - {} - {}\n'.format(id, e, date_str))
			return None
		else:
			return (date_obj.strftime('%Y-%m-%d'), pages)