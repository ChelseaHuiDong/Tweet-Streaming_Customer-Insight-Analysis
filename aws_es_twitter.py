import tweepy  
import sys  
import json  
from textwrap import TextWrapper  
from datetime import datetime  
from elasticsearch import Elasticsearch


consumer_key="H8Fbaj3LBTzCiI1ICTs43bH92"  
consumer_secret="WXNaRK3TfWFTFvcqhIPoB6IQhyzzPiv9gEkEDFTW55Mb8LjEu2"

access_token="324458317-wOpvuBl1pXF20Tr3isD83UrR7l21qe7kvIDS5KFA"  
access_token_secret="iNVfAPRqN7QzvWnGa7GU3W1Vo9IkXIUQ4cJL7DEaxhyLW"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)  
auth.set_access_token(access_token, access_token_secret)


from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

AWS_ACCESS_KEY = 'AKIAJEM2Z2WULFV2RZQA'
AWS_SECRET_KEY = '+aq3APqeCjpPDUbCWoQ8RTpHNL9+Bu6d8l2e02/g'
region = 'us-east-1' # For example, us-east-1
service = 'es'

awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)

host = 'search-es-bigdata-project1-q7njkr4po2wl6uiyp3sqa5dinm.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
#host = 'vpc-bigdata-project-5wnppsjtigm6j457j7bs62x7ui.us-east-1.es.amazonaws.com'

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)


class StreamListener(tweepy.StreamListener):  
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        try:
            #print 'n%s %s' % (status.author.screen_name, status.created_at)

            json_data = status._json
            #print json_data['text']

            es.create(index="idx_twp",
                      doc_type="twitter_twp",
                      body=json_data
                     )

        except Exception, e:
            print e
            pass

streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=3000000000 )

#Fill with your own Keywords bellow
terms = ['apple iphonex']

streamer.filter(None,terms)  
#streamer.userstream(None)