from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index
from elasticsearch_dsl import connections

connections.create_connection(hosts=['localhost'], timeout=20)

i = Index('companies')
#i.settings(number_of_shards=1)
#i.create()
print(i.stats())
r = i.search().scan()
print(next(r))
