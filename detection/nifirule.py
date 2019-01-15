#!/usr/bin/env python
"""This searches for incidents regarding ApacheNifif and classifies them."""

__author__ = 'Manfred Halper'
__license__ = 'Technische Universität Wien'
__date__ = '21.12.2017'

#connect to our cluster
from datetime import datetime
from elasticsearch import Elasticsearch
from neo4jrestclient.client import GraphDatabase

def change_events():
    """Adds the events to the corresponding incidents"""
    for hit in RESPONSE['hits']['hits']:
        if "HDFS" in hit['_source']['message']:
            print('HDFS error:' + hit['_source']['errorcode'] + hit['_id'])
            if 'events' in data:
                data['events'].append(hit['_id'])
            else:
                data['events'] = [hit['_id']]
            CLIENT.update(index='nifilogfiles',\
                    doc_type='doc',\
                    id=hit['_id'],\
                    body={
                        "script" : "ctx._source.partofincident = 'YES'"
                        }
                         )


def classify_incident():
    """Classifies the data regarding the neo4j database"""
    data['owner'] = (DATABASE.query(('MATCH (nifi {name: "ApacheNifi"})\
                                 <-[:OWNS]- (provider) \
                                 RETURN provider.name'),\
                                 data_contents=True)[0][0])
    data['phase'] = (DATABASE.query(('MATCH (nifi {name: "ApacheNifi"})\
                                    -[:BELONGSTO]-(phase)\
                                    -[:IS]-(:Element{name:"Analytics phase"})\
                                    RETURN phase.name'),\
                                    data_contents=True)[0][0])
    data['functionalarea'] = (DATABASE.query(('MATCH (nifi {name: "ApacheNifi"})\
                                                -[:BELONGSTO]-(function)\
                                                -[:IS]-(:Element{name:"Functional area"})\
                                                RETURN function.name'),\
                                                data_contents=True)[0][0])
    data['assetstate'] = (DATABASE.query(('MATCH (nifi {name: "ApacheNifi"})\
                                            -[:ASSETSTATE]-(asset)\
                                            -[:HASSTATE]-(:Element{name:"Data asset"})\
                                            RETURN asset.name'),\
                                            data_contents=True)[0][0])
    data['predeccessor'] = (DATABASE.query(('MATCH (pipes:DataPipeline)\
                                            -[relatedTo]->\
                                            (:DataPipeline {name: "ApacheNifi"})\
                                            RETURN pipes.name'),\
                                            data_contents=True)[0][0])
    data['sucessor'] = (DATABASE.query(('MATCH (pipes:DataPipeline)\
                                            <-[relatedTo]-\
                                            (:DataPipeline {name: "ApacheNifi"})\
                                            RETURN pipes.name'),\
                                            data_contents=True)[0][0])
    data['effect'] = 'reduction of quality'
    data['cause'] = 'unknown'
    data['date'] = (datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

def create_incident():
    """Creates an incident"""
    result = CLIENT.index(index="incident", doc_type='doc', body=data)
    print(result['created'])

#Instantiate Database connections
CLIENT = Elasticsearch([{'host': 'localhost', 'port': 9200}])
DATABASE = GraphDatabase("http://localhost:7474", username="neo4j", password="Ztu5C!:")
data = {}

#Search for incidents
RESPONSE = CLIENT.search(
    index='nifilogfiles', body={
        "size" : 100, "query": {
            'bool' : {
                'must': [
                    {"match": {'loglevel':'ERROR'}},
                    {"match": {'message':'HDFS'}}
                ],
                'must_not': [
                    {"match": {'partofincident':'YES'}}
                ]
            }
        }
    })

if RESPONSE['hits']['hits']:
    print("Incident")
    classify_incident()
    change_events()
    create_incident()
else:
    print("No Incident found.")
