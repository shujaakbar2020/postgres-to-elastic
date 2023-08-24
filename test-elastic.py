
########################################################################################

from elasticsearch import Elasticsearch

# Define your Elasticsearch server's URL
elasticsearch_url = 'http://103.83.89.197:9200'

# Create an Elasticsearch instance with authentication
es = Elasticsearch(
    [elasticsearch_url],
    http_auth=('elastic', 'mevris123')
)

# Define your search query
search_query = {
    "query": {
        "match_all": {}
    }
}

# Perform the search
result = es.search(index='test', body=search_query)

# Process the results
for hit in result['hits']['hits']:
    print(hit['_source'])
