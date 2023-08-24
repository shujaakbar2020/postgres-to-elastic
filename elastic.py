import psycopg2
from psycopg2 import sql
from elasticsearch import Elasticsearch, helpers
# PostgreSQL Configuration
pg_host = '103.83.89.197'
pg_port = '5432'
pg_database = 'mevris-entity-data-db'
pg_user = 'postgres'
pg_password = 'mevris@123'
pg_table = 'EntityStates'
pg_connection = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=pg_database,
    user=pg_user,
    password=pg_password
)
# Create a PostgreSQL cursor
pg_cursor = pg_connection.cursor()
# Fetch data from PostgreSQL table
query = sql.SQL("SELECT * FROM {} LIMIT 10").format(sql.Identifier(pg_table))
pg_cursor.execute(query)
entity_states = pg_cursor.fetchall()
# Close PostgreSQL cursor and connection
pg_cursor.close()
pg_connection.close()

##############################################################################

state_list = []

for state in entity_states:
    result = {
        "id": state[0],
        "cursorId": state[1],
        "createdAt": state[2].strftime('%Y-%m-%d %H:%M:%S'),
        "updatedAt": state[3].strftime('%Y-%m-%d %H:%M:%S'),
        "deletedAt": state[4].strftime('%Y-%m-%d %H:%M:%S')  if state[4] is not None else None,
        "version": state[5],
        "key": state[6],
        "value": state[7],
        "dataType": state[8],
        "time": state[9].strftime('%Y-%m-%d %H:%M:%S'),
        "entityId": state[10],
        "eventIds": state[11],
        "requestId": state[12]
    }
    state_list.append(result)

##############################################################################

from elasticsearch import Elasticsearch

# Define your Elasticsearch server's URL
elasticsearch_url = 'http://103.83.89.197:9200'

# Create an Elasticsearch instance with authentication
es = Elasticsearch(
    [elasticsearch_url],
    # bearer_auth = 'aUwxVUpvb0Jvd1dRSmF6bl9oZlk6dlZES2tYZVVRNHFWSWNnZjJxZ1Y3UQ=='
    basic_auth=('elastic', 'mevris123')
)

actions = [
    {
        "_index": "entity-state",
        "_source": doc
    }
    for doc in state_list
]

helpers.bulk(es, actions)
