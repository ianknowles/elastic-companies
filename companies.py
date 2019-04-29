from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])


class Company(Document):
	name = Text(analyzer='snowball', fields={'raw': Keyword()})
	body = Text(analyzer='snowball')
	tags = Keyword()
	founded = Date()
	lines = Integer()

	class Index:
		name = 'companies'
		settings = {
			"number_of_shards": 1,
		}

	def save(self, ** kwargs):
		#self.lines = len(self.body.split())
		return super(Company, self).save(** kwargs)

	def is_published(self):
		return datetime.now() >= self.published_from


if __name__ == "__main__":
	# create the mappings in elasticsearch
	Company.init()

	import csv

	row_count = 0
	with open('company_list.tsv', 'r', encoding='utf8') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')

		for row in tsvin:
			row_count += 1

	print('{time} Rows to ingest {x}'.format(x=row_count, time=datetime.now()))

	row_count = 0
	with open('company_list.tsv', 'r', encoding='utf8') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')

		for row in tsvin:
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
