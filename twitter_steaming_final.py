from tweepy.streaming import StreamListener
from tweepy.auth import OAuthHandler
from tweepy import Stream
import sys  
import json  
from textwrap import TextWrapper  
from datetime import datetime  
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from httplib import IncompleteRead
from urllib3.exceptions import ProtocolError


reload(sys)
sys.setdefaultencoding('utf8')

consumer_key = '***'
consumer_secret = '***'

access_token = '***'
access_token_secret = '***'

AWS_ACCESS_KEY = '***'
AWS_SECRET_KEY = '***'
region = 'us-west-2' # For example, us-east-1
service = 'es'

auth = OAuthHandler(consumer_key, consumer_secret)  
auth.set_access_token(access_token, access_token_secret)

awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)

host = '***'

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection)

#Fill with your own Keywords bellow
terms = ['iphone', 'i phone', 'iphones', 'i phones', 'applephone', 'apple phone', 'applephones', 'apple phones', 'apple i phone', 'apple i phones', 'applei phone', 'applei phones', 'apple iphone', 'apple iphones', 'appleiphone', 'appleiphones', 'new apple phone', 'new apple phones', 'newapple phone', 'newapple phones', 'new applephone', 'new applephones', 'newapplephone', 'newapplephones']
terms += ['new google pixel', 'newgoogle pixel', 'new googlepixel', 'newgooglepixel', 'new google pixels', 'newgoogle pixels', 'new googlepixels', 'newgooglepixels', 'new google phone', 'newgoogle phone', 'new googlephone', 'newgooglephone', 'new google phones', 'newgoogle phones', 'new googlephones', 'newgooglephones', 'new pixel phone', 'newpixel phone', 'new pixelphone', 'newpixelphone', 'new pixel phones', 'newpixel phones', 'new pixelphones', 'newpixelphones']
terms += ['pixel 2', 'pixel 2s', 'pixel2', 'pixel2s', 'pixel 2 xl', 'pixel 2 xls', 'pixel2 xl', 'pixel2 xls', 'pixel 2xl', 'pixel 2xls', 'pixel2xl', 'pixel2xls', 'pixel xl 2', 'pixel xl 2s', 'pixelxl 2', 'pixelxl 2s', 'pixel xl2', 'pixel xl2s', 'pixelxl2', 'pixelxl2s']
terms += ['galaxy note 8', 'galaxy note 8s', 'galaxynote 8', 'galaxynote 8s', 'galaxy note8', 'galaxy note8s', 'galaxynote8', 'galaxynote8s', 'galaxy 8 note', 'galaxy 8 notes', 'galaxy8 note', 'galaxy8 notes', 'galaxy 8note', 'galaxy 8notes', 'galaxy8note', 'galaxy8notes', 'galaxys note 8', 'galaxys note 8s', 'galaxysnote 8', 'galaxysnote 8s', 'galaxys note8', 'galaxys note8s', 'galaxysnote8', 'galaxysnote8s', 'galaxys 8 note', 'galaxys 8 notes', 'galaxys8 note', 'galaxys8 notes', 'galaxys 8note', 'galaxys 8notes', 'galaxys8note', 'galaxys8notes']
terms += ['new galaxy phone', 'newgalaxy phone', 'new galaxyphone',  'newgalaxyphone', 'new galaxy phones', 'newgalaxy phones', 'new galaxyphones',  'newgalaxyphones', 'new galaxys phone', 'newgalaxys phone', 'new galaxysphone',  'newgalaxysphone', 'new galaxys phones', 'newgalaxys phones', 'new galaxysphones',  'newgalaxysphones']
terms += ['galaxy s 8', 'galaxy s 8s', 'galaxys 8', 'galaxys 8s', 'galaxy s8', 'galaxy s8s', 'galaxys8', 'galaxys8s', 'galaxy 8 s', 'galaxy 8 ss', 'galaxy8 s', 'galaxy8 ss', 'galaxy 8s', 'galaxy 8ss', 'galaxy8s', 'galaxy8ss', 'galaxys s 8', 'galaxys s 8s', 'galaxyss 8', 'galaxyss 8s', 'galaxys s8', 'galaxys s8s', 'galaxyss8', 'galaxyss8s', 'galaxys 8 s', 'galaxys 8 ss', 'galaxys8 s', 'galaxys8 ss', 'galaxys 8ss', 'galaxys8ss']
terms += ['galaxy plus 8', 'galaxy plus 8s', 'galaxyplus 8', 'galaxyplus 8s', 'galaxy plus8', 'galaxy plus8s', 'galaxyplus8', 'galaxyplus8s', 'galaxy 8 plus', 'galaxy 8 pluses', 'galaxy8 plus', 'galaxy8 pluses', 'galaxy 8plus', 'galaxy 8pluses', 'galaxy8plus', 'galaxy8pluses', 'galaxys plus 8', 'galaxys plus 8s', 'galaxysplus 8', 'galaxysplus 8s', 'galaxys plus8', 'galaxys plus8s', 'galaxysplus8', 'galaxysplus8s', 'galaxys 8 plus', 'galaxys 8 pluses', 'galaxys8 plus', 'galaxys8 pluses', 'galaxys 8plus', 'galaxys 8pluses', 'galaxys8plus', 'galaxys8pluses']
terms += ['galaxy s 8 plus', 'galaxy s 8 pluses', 'galaxy s 8plus', 'galaxy s 8pluses', 'galaxy s8 plus', 'galaxy s8 pluses', 'galaxy s8plus', 'galaxy s8pluses',  'galaxys s 8 plus', 'galaxys s 8 pluses', 'galaxys s 8plus', 'galaxys s 8pluses', 'galaxys s8 plus', 'galaxys s8 pluses', 'galaxyss 8 plus', 'galaxyss 8 pluses', 'galaxys s8plus', 'galaxys s8pluses', 'galaxyss 8plus', 'galaxyss 8pluses', 'galaxyss8 plus', 'galaxyss8 pluses', 'galaxyss8plus', 'galaxyss8pluses']
terms += ['galaxy plus s 8', 'galaxy plus s 8s', 'galaxy plus s8', 'galaxy plus s8s', 'galaxy pluss 8', 'galaxy pluss 8s', 'galaxyplus s 8', 'galaxyplus s 8s', 'galaxyplus s8', 'galaxyplus s8s', 'galaxy pluss8', 'galaxy pluss8s', 'galaxypluss 8', 'galaxypluss 8s', 'galaxypluss8', 'galaxypluss8s', 'galaxys plus s 8', 'galaxys plus s 8s', 'galaxys plus s8', 'galaxys plus s8s', 'galaxys pluss 8', 'galaxys pluss 8s', 'galaxysplus s 8', 'galaxysplus s 8s', 'galaxysplus s8', 'galaxysplus s8s', 'galaxys pluss8', 'galaxys pluss8s', 'galaxyspluss 8', 'galaxyspluss 8s', 'galaxyspluss8', 'galaxyspluss8s']
terms += ['galaxy + 8', 'galaxy + 8s', 'galaxy+ 8', 'galaxy+ 8s', 'galaxy +8', 'galaxy +8s', 'galaxy+8', 'galaxy+8s', 'galaxy 8 +', 'galaxy 8 +s', 'galaxy8 +', 'galaxy8 +s', 'galaxy 8+', 'galaxy 8+s', 'galaxy8+', 'galaxy8+s', 'galaxys + 8', 'galaxys + 8s', 'galaxys+ 8', 'galaxys+ 8s', 'galaxys +8', 'galaxys +8s', 'galaxys+8', 'galaxys+8s', 'galaxys 8 +', 'galaxys 8 +s', 'galaxys8 +', 'galaxys8 +s', 'galaxys 8+', 'galaxys 8+s', 'galaxys8+', 'galaxys8+s']
terms += ['galaxy s 8 +', 'galaxy s 8 +s', 'galaxy s 8+', 'galaxy s 8+s', 'galaxy s8 +', 'galaxy s8 +s', 'galaxy s8+', 'galaxy s8+s',  'galaxys s 8 +', 'galaxys s 8 +s', 'galaxys s 8+', 'galaxys s 8+s', 'galaxys s8 +', 'galaxys s8 +s', 'galaxyss 8 +', 'galaxyss 8 +s', 'galaxys s8+', 'galaxys s8+s', 'galaxyss 8+', 'galaxyss 8+s', 'galaxyss8 +', 'galaxyss8 +s', 'galaxyss8+', 'galaxyss8+s']
terms += ['galaxy + s 8', 'galaxy + s 8s', 'galaxy + s8', 'galaxy + s8s', 'galaxy +s 8', 'galaxy +s 8s', 'galaxy+ s 8', 'galaxy+ s 8s', 'galaxy+ s8', 'galaxy+ s8s', 'galaxy +s8', 'galaxy +s8s', 'galaxy+s 8', 'galaxy+s 8s', 'galaxy+s8', 'galaxy+s8s', 'galaxys + s 8', 'galaxys + s 8s', 'galaxys + s8', 'galaxys + s8s', 'galaxys +s 8', 'galaxys +s 8s', 'galaxys+ s 8', 'galaxys+ s 8s', 'galaxys+ s8', 'galaxys+ s8s', 'galaxys +s8', 'galaxys +s8s', 'galaxys+s 8', 'galaxys+s 8s', 'galaxys+s8', 'galaxys+s8s']
	

class StreamListener(StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
    def on_status(self, status):
        try:
            #print 'n%s %s' % (status.author.screen_name, status.created_at)

            json_data = status._json
            #print json_data['text']
            raiseFieldLimit = '''
            {  
              "index.mapping.total_fields.limit": 10000
            }'''
	
            es.indices.create(index='idx_twp_new2', ignore=400, body=raiseFieldLimit)

            es.index(index="idx_twp_new2", doc_type="twitter_twp", id = json_data['id'], body=json_data)

        except Exception, e:
            print e
            pass

while True:
    try:
        streamer = Stream(auth=auth, listener=StreamListener(), timeout=3000000000000000000000000000000000000)
        streamer.filter(None,terms)
        #streamer.userstream(None)
    except ProtocolError:
        continue
    except IncompleteRead:
        continue
    except KeyboardInterrupt:
        stream.disconnect()
        break
