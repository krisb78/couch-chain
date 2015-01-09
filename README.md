# couch-chain - a tool for processing couchdb's \_changes stream


## Introduction

This package is meant for basic processing of the \_changes stream that 
[couchdb](http://docs.couchdb.org/en/latest/api/database/changes.html) 
and it's distributed version, 
[Cloudant](https://docs.cloudant.com/api/database.html?highlight=_changes#obtaining-a-list-of-changes), provide.

I wrote it primarily to have an easy way of indexing my data in 
[Elasticsearch](http://www.elasticsearch.org/),
as [couchdb-river](https://github.com/elasticsearch/elasticsearch-river-couchdb) is being deprecated.


## Disclaimer

This package is meant as a proof of concept, rather than anything else. 
It is purely synchronous, and therefore efficiency is not it's strongest 
point. My primary focus was to have a way of writing changes processors 
quickly. At present I don't need to have this super-efficient.


## Usage


### Indexing documents in Elasticsearch

Assuming that you have both ElasticSearch and Couchdb running locally,
you could index your documents like so:

```python

import cchain


processor = cchain.processors.es.SimpleESChangesProcessor(
    ['http://localhost:9200'],  # locations of your ES nodes
    'my_index',  # the name of the index
    'my_doc_type'  # doc type to set on documents in ES (_type).
)

seqtracker = cchain.seqtrackers.base.FilebasedSeqTracker(
    'indexing.seq'
)

consumer = cchain.consumers.base.BaseChangesConsumer(
    'http://localhost:5984',  # location of your Couchdb server
    'my_database',  # the name of the database to index
    processor=processor,
    seqtracker=seqtracker
)

consumer.consume()

```


### Fixing database inconsistencies in Couchdb


TODO.
