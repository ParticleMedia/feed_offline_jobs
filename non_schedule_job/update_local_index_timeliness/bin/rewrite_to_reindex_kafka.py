#!/usr/bin/env python
# encoding: utf-8

from kafka import KafkaProducer
from pymongo import MongoClient
import json

KAFKA_HOSTS = '172.24.21.112:9092,172.24.19.184:9092,172.24.16.186:9092'
KAFKA_TOPIC = 'nonnews_reindex_document'

kafka_servers = KAFKA_HOSTS.split(',')
kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=kafka_servers)

def write_kafka(doc_id, url):
    data = {'docId': doc_id, 'url': url}
    res = kafka_producer.send(KAFKA_TOPIC, data)
    kafka_producer.flush()
    #print(res)

def get_url(col, doc_id):
    data_dict = col.find_one({'_id': doc_id}, {'url':1, 'timeliness_type':1, 'data':1, 'expire_time':1})
    url = ''
    if 'url' in data_dict:
        url = data_dict['url']
    return url

def load_data():
    filename = '../log/update_timeliness.log'
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            splits = line.strip().split('docid=')
            if len(splits) == 1:
                continue
            parts = splits[1].split(' ')
            docid = parts[0]
            timeliness = parts[1]
            msg = parts[2]
            #print('\t{}\t{}\t{}'.format(docid, timeliness, msg))
            if msg == 'msg=none':
                data[docid] = timeliness
    return data

def main():
    staticfeature_col = MongoClient('172.31.29.170', 27017, unicode_decode_error_handler='ignore')['staticFeature']['document']
    docids = load_data()
    tot = len(docids)
    idx = 0
    print('load data={}'.format(tot))
    for docid, timeliness in docids.items():
        idx += 1
        url = get_url(staticfeature_col, docid)
        write_kafka(docid, url)
        print('write\t{}/{}\t{}\t{}\t{}'.format(idx, tot, docid, timeliness, url))

if __name__ == '__main__':
    main()
