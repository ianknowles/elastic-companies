import csv
import os
import shutil
import urllib.request
import zipfile
from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections

companies_zip_url = 'http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2019-04-01.zip'
companies_zip_file = 'BasicCompanyDataAsOneFile-2019-04-01.zip'
# Directory objects?
file_path = os.path.dirname(os.path.realpath(__file__))
project_path = os.path.normpath(os.path.join(file_path))
data_path = os.path.join(project_path, 'data')
companies_csv_filename = 'BasicCompanyDataAsOneFile-2019-04-01.csv'
companies_csv_filepath = os.path.join(data_path, companies_csv_filename)

connections.create_connection(hosts=['localhost'])


class Company(Document):
	"""Class representing the searchable companies metadata"""
	name = Text(analyzer='snowball', fields={'raw': Keyword()})
	body = Text(analyzer='snowball')
	tags = Keyword()
	founded = Date()
	lines = Integer()

	class Index:
		"""The index that all instances of this metadata will be saved to"""
		name = 'companies'
		settings = {
			"number_of_shards": 1,
		}

	def save(self, ** kwargs):
		"""Saves the current item to the index"""
		#self.lines = len(self.body.split())
		return super(Company, self).save(** kwargs)

	def is_published(self):
		return datetime.now() >= self.published_from


def download():
	"""Download the csv from companies house"""
	print('{time} Downloading companies house zip'.format(time=datetime.now()))
	with urllib.request.urlopen(companies_zip_url) as response, open(companies_zip_file, 'wb') as out_file:
		shutil.copyfileobj(response, out_file)


def unzip():
	print('{time} Unzipping companies house zip'.format(time=datetime.now()))
	with zipfile.ZipFile(companies_zip_file, 'r') as zipfilename:
		zipfilename.extractall("data")


def company_count():
	"""Check how many rows to ingest"""
	row_count = 0
	with open(companies_csv_filepath, 'r', encoding='utf8') as csvin:
		companies = csv.reader(csvin)

		for company in companies:
			row_count += 1

	print('{time} Rows to ingest {x}'.format(x=row_count, time=datetime.now()))


def ingest():
	"""Ingest everything in the csv into the cluster"""
	# create the mappings in elasticsearch
	Company.init()

	row_count = 0
	with open(companies_csv_filepath, 'r', encoding='utf8') as csvin:
		companies = csv.reader(csvin)

		for row in companies:
			try:
				company = Company(name=row[0])
				company.save()
				row_count += 1
				if (row_count % 1000) == 0:
					print('{time} Ingested {x}'.format(x=row_count, time=datetime.now()))
			except UnicodeDecodeError:
				print('{time} unicode error'.format(time=datetime.now()))


	# create and save and article
	#article = Company(meta={'id': 42}, title='Hello world!', tags=['test'])
	#article.body = ''' looong text '''
	#article.published_from = datetime.now()
	#article.save()

	#article = Company.get(id=42)
	#print(article.is_published())

	# Display cluster health
	print(connections.get_connection().cluster.health())


if __name__ == "__main__":
	if not os.path.isfile(companies_zip_file):
		download()
	if not os.path.isfile(companies_csv_filepath):
		unzip()

	company_count()

	from elasticsearch_dsl import Index
	i = Index('companies')
	i.delete()

	ingest()
