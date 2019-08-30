# elastic-companies
Download the MSI from https://www.elastic.co/downloads/elasticsearch

Install and follow the install steps at the download URL

Ensure your HDD has more than 5% free space, ElasticSearch locks down nodes when space is below 5% to avoid filling your hdd

Run the Elastic Search service


Run companies.py to ingest data, 20 hours run time approx. Requires the data subfolder to exist and be writeable.

Run query.py for matches, expects a file named example 'example_for_matching.tsv' at the project top level containing a list of strings to query, one string per line.

## TODO
Integrating new data into an existing cluster.