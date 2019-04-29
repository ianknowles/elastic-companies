from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import connections

connections.create_connection(hosts=['localhost'], timeout=20)

client = Elasticsearch()

s = Search(using=client, index='companies').query("match", name="ROYAL CHARTER")
	#.filter("term", category="search") \
#s.query("match", name="43")
	#.exclude("match", description="beta")

#s.aggs.bucket('per_tag', 'terms', field='tags') \
#	.metric('max_lines', 'max', field='lines')

response = s.execute()

print(response)

for hit in response:
	print(hit.meta.score, hit.name)

#for tag in response.aggregations.per_tag.buckets:
#	print(tag.key, tag.max_lines.value)

import csv
import datetime

responses = []
with open('example_for_matching.tsv', 'r', encoding='utf8') as tsvin:
	tsvin = csv.reader(tsvin, delimiter='\t')

	try:
		for row in tsvin:
			s = Search(using=client, index='companies').query("match", name=row[0])
			response = s.execute()
			result_row = {'query_string': row[0]}
			for n, hit in enumerate(response):
				result_row['match_{n}'.format(n=n)] = hit.name
				result_row['score_{n}'.format(n=n)] = hit.meta.score
			responses += [result_row]
	except UnicodeDecodeError:
		print('{time} unicode error. q={q}'.format(time=datetime.datetime.now(), q=row[0]))

with open('matches.csv', 'w', encoding='utf8') as csvout:
	#csvout = csv.DictWriter
	fieldnames = ['query_string',
				  'match_0', 'score_0',
				  'match_1', 'score_1',
				  'match_2', 'score_2',
				  'match_3', 'score_3',
				  'match_4', 'score_4',
				  'match_5', 'score_5',
				  'match_6', 'score_6',
				  'match_7', 'score_7',
				  'match_8', 'score_8',
				  'match_9', 'score_9',
				  ]
	writer = csv.DictWriter(csvout, fieldnames=fieldnames)

	writer.writeheader()
	writer.writerows(responses)
