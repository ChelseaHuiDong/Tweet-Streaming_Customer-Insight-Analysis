from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

AWS_ACCESS_KEY = '***'
AWS_SECRET_KEY = '***'
region = 'us-east-1' # For example, us-east-1
service = 'es'

awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)

host = '***' # For example, my-test-domain.us-east-1.es.amazonaws.com

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

document = {
    "title": "Moneyball",
    "director": "Bennett Miller",
    "year": "2011"
}

es.index(index="movies", doc_type="movie", id="5", body=document)

print(es.get(index="movies", doc_type="movie", id="5"))
