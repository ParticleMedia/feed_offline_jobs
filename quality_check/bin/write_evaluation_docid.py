import json
import sys
import os
from pymongo import MongoClient
import requests
import datetime
import logging
from kafka import KafkaProducer
import json

kRS2 = "172.31.20.160,172.31.20.161,172.31.20.162"
kRS3 = "172.24.25.74,172.24.31.222,172.31.29.170,172.31.24.237,172.31.24.51"

RET_SUCC_WRITE = 0
RET_EXIST = 1
RET_ERROR = -1

def get_collection_static_feature_document():
    client = MongoClient(kRS3)
    col = client['staticFeature']['document']
    return col

def get_collection_display():
    client = MongoClient(kRS2)
    col = client['serving']['displayDocument']
    return col

def get_collection_quality_check():
    client = MongoClient( 'mongodb://video.mongo.nb.com:27017/?replicaSet=rs_video&readPreference=secondaryPreferred')
    col = client['content']['quality_check']
    return col

class TopCheckManager:
    def __init__(self):
        self.static_feature_col = get_collection_static_feature_document()
        self.serving_col = get_collection_display()
        self.quality_check_col = get_collection_quality_check()

        self.kafka_servers = ["172.24.30.130:9092", "172.24.21.47:9092", "172.24.25.19:9092", "172.24.19.108:9092", "172.24.17.81:9092", "172.24.28.126:9092", "172.24.20.127:9092", "172.24.21.241:9092"]
        self.kafka_topic = 'doc_review'
        self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=self.kafka_servers)

    def hasLabeled(self, doc_id):
        data = self.quality_check_col.find_one({"_id": doc_id})
        return data != None

    def formatData(self, doc_id):
        data = self.serving_col.find_one({"_id": doc_id})
        format_dict = {}
        format_dict['_id'] = doc_id
        format_dict['type'] = "quality_check"
        format_dict['doc'] = {}
        if "mp_full_article" in data:
            format_dict['mp_full_article'] = data["mp_full_article"]
        else:
            format_dict['mp_full_article'] = False
        if 'date' not in data:
            format_dict['doc']['date'] = ""
            logging.info("missing date in " + data['_id'])
        else:
            format_dict['doc']['date'] = data['date']
        format_dict['doc']['title'] = data.get('title', '')
        if format_dict['mp_full_article'] is True:
            format_dict['doc']["url"] = "https://h5.newsbreakapp.com/mp/{}?noRedirect=1".format(format_dict['_id'])
            logging.info("got mp url " + format_dict['doc']["url"])
        else:
            format_dict['doc']['url'] = data.get('url', '')
        format_dict['doc']['image_urls'] = []
        for img in data.get('image_urls', []):
            format_dict['doc']['image_urls'].append("https://img.particlenews.com/image.php?url={}".format(img))
        format_dict['doc']['content'] = data.get('content', '').replace('http://image1.hipu.com', 'https://img.particlenews.com')
        format_dict['insert_time'] = datetime.datetime.utcnow()
        return format_dict

    def sendToKafka(self, doc_id):
        data = {
                "docid" : doc_id,
                "queue" : "quality_check",
                "tag" : ["from_foryou"]
                }
        self.producer.send(self.kafka_topic, data)

    def write_docid(self, doc_id):
        try:
            if self.hasLabeled(doc_id):
                return (RET_EXIST, "exist")
            else:
                self.sendToKafka(doc_id)
                return (RET_SUCC_WRITE, "succ")
        except BaseException as e:
            return (RET_ERROR, "error:{}".format(e))

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

def load_wrote_docid_set(wrote_docid_file):
    wrote_docid_set = set()
    if os.path.exists(wrote_docid_file):
        with open(wrote_docid_file, 'r') as f:
            for line in f:
                ws = line.strip().split('\t')
                docid = ws[0]
                wrote_docid_set.add(docid)
    return wrote_docid_set

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("wrong input parameter")
        sys.exit(1)

    input_docid_file = sys.argv[1]
    wrote_docid_file = sys.argv[2]
    log_file = sys.argv[3]

    logging.basicConfig(filename=log_file, filemode="a", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    succ_num, exist_num, error_num, dup_num, skip_num = (0, 0, 0, 0, 0)
    top_check_manager = TopCheckManager()

    docid_set = set()
    wrote_docid_set = load_wrote_docid_set(wrote_docid_file)
    logging.info("load wrote_docid_set={}".format(len(wrote_docid_set)))

    with open(input_docid_file, 'r') as f, open(wrote_docid_file, 'a+') as fw:
        lines = f.readlines()
        for i in range(len(lines)):
            line = lines[i]
            elems = line.strip().split("\t")
            if len(elems) < 2:
                skip_num += 1
                continue
            doc_id = elems[1]

            # filter duplicate docid
            if (doc_id in docid_set) or (doc_id in wrote_docid_set):
                logging.info("duplicate_docid lineno={} docid={}".format(i, doc_id))
                dup_num += 1
                continue

            (ret, msg) = top_check_manager.write_docid(doc_id)
            if ret == RET_SUCC_WRITE:
                logging.info("succ_write lineno={} docid={}".format(i, doc_id))
                fw.write('{}\tfrom={}\n'.format(doc_id, input_docid_file))
                docid_set.add(doc_id)
                succ_num += 1
            elif ret == RET_EXIST:
                logging.info("already_exist lineno={} docid={}".format(i, doc_id))
                docid_set.add(doc_id)
                exist_num += 1
            else:
                logging.info("write_error lineno={} docid={} error={}".format(i, doc_id, msg))
                error_num += 1

    logging.info("finished_statics: succ_num={} exist_num={} dup_num={} skip_num={} error_num={}".format(succ_num, exist_num, dup_num, skip_num, error_num))
    top_check_manager.producer.close()
