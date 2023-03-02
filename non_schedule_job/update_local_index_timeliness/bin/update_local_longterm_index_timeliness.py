#encoding=utf8

import logging

from elasticsearch import Elasticsearch

INDEX = 'local-longterm-doc'
HOSTS = ['http://local-es.ha.nb.com:9200']
# HOSTS = ['http://172.20.0.38:9200']
DOC_TYPE = 'news'

def load_data(filename):
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            splits = line.strip().split('\001')
            if len(splits) != 2:
                continue
            docid = splits[0].strip()
            timeliness = splits[1].strip()
            data[docid] = timeliness
    print_log('load data={}'.format(len(data)))
    return data

def get_index(es, docid):
    try:
        res = es.get(index=INDEX, doc_type=DOC_TYPE, id=docid)
    except Exception as e:
        # print too much message
        # logging.error('error for get docid=' + docid + ' error=', e)
        return False
    return True

def search_index(es, docid):
    try:
        query = {
          "query": {
            "match": {
              "_id": docid
            }
          }
        }

        res = es.search(index=INDEX, doc_type=DOC_TYPE, body=query, filter_path=['hits.hits.*'])
        # print(res)
        items = res.get('hits', {}).get('hits', [])
        if len(items) == 0:
            return False
        else:
            return True
    except Exception as e:
        logging.error('error for search docid=' + docid + ' error=', e)
        return False
    return True

def update_index(es, docid, timeliness):
    try:
        query = {"doc": {"timeliness_type": timeliness}}
        res = es.update(index=INDEX, doc_type=DOC_TYPE, id=docid, body=query)
    except Exception as e:
        logging.error('error for update docid='+docid+ ' error=', e)
        return False
    return True

def print_log(s):
    print(s)
    logging.info(s)

def main():
    filename = '../data/data.all'
    log_file = '../log/update_timeliness.log'
    logging.basicConfig(filename=log_file, filemode="w", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    data = load_data(filename)
    es = Elasticsearch(HOSTS)
    doc_cnt, in_es_cnt, succ_update_es_cnt = 0, 0, 0
    for k,v in data.items():
        doc_cnt+=1
        msg = 'none'
        if doc_cnt % 1000 == 0:
            print_log("process doc={}".format(doc_cnt))
        res = search_index(es, k)
        if res:
            in_es_cnt += 1
            msg = 'in_es'
            res = update_index(es, k, v)
            if res:
                msg = 'succ_update_es'
                succ_update_es_cnt += 1
        logging.info('docid={} {} msg={}'.format(k, v, msg))
    print_log("finished. total={} in_es={} succ_update_es={}".format(doc_cnt, in_es_cnt, succ_update_es_cnt))

if __name__ == '__main__':
    main()
