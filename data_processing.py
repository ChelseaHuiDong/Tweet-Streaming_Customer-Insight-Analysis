from elasticsearch import Elasticsearch

es = Elasticsearch({"host": "localhost", "port": 9200}) #many other settings are available if using https and so on

query = {
        "query": {
            "match" : {
                "CourseInfo.subjectKeywords" : "foo"
            }
        }
    }
res = es.search(index="educationforumsenriched2", body=query)

#do some processing

#create new document in ES
es.create(index="educationforumsenriched2", body=new_doc_after_processing)