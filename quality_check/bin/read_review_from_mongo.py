import json
import sys
from pymongo import MongoClient
import requests
import datetime
import logging
import os

def get_collection_top_check():
    client = MongoClient( 'mongodb://video.mongo.nb.com:27017/?replicaSet=rs_video&readPreference=secondaryPreferred')
    # col = client['content']['top_checked']
    col = client['content']['quality_check']
    return col

class ReadManager:
    def __init__(self):
        self.top_check_col = get_collection_top_check()
        self.doc_dict = {}

    def searchOne(self, doc_id):
        # data = self.top_check_col.find_one({"_id" : doc_id})
        data = self.top_check_col.find_one({"_id" : doc_id + "_from_foryou"})
        msg = 'null'
        editor = "null"
        if data is None:
            #data = self.top_check_col.find_one({"_id" : doc_id})
            return ('error', ("null", "not_in_mongo", editor))
        try:
            # print(data)
            if "review" in data:
                review_obj = data["review"]
                reason = "null"

                if "editor" in review_obj:
                    editor = review_obj["editor"]
                if "status" in review_obj:
                    status = review_obj["status"]
                    if "reason"  in review_obj:
                        reason = review_obj["reason"]
                    return ('succ', (status, reason, editor))
                else:
                    msg = "no_status"
            else:
                msg = "no_review"
        except BaseException as e:
            return ('error',  ("null", "error_"+str(e), editor))
        return ('error',  ("null", msg, editor))

    def readDict(self, input_filename):
        doc_dict = {}
        wrong_cnt = 0
        empty_cnt = 0
        succ_cnt = 0
        line_cnt = 0
        with open(input_filename, 'r') as in_f:
            for line in in_f.readlines():
                line_cnt += 1
                line = line.strip()
                elems = line.split("\t")
                if len(elems) <= 2:
                    logging.info("wrong input:{}".format(line))
                    wrong_cnt += 1
                    continue
                userid = elems[0]
                docid = elems[1]
                (ret, (status, reason, editor)) = read_manager.searchOne(docid)
                if ret == 'succ':
                    doc_dict[docid] = (status, reason)
                    succ_cnt += 1
                else:
                    empty_cnt += 1
                li = [str(line_cnt), ret, docid, status, reason, editor]
                logging.info("read_data\t{}".format('\t'.join(li)))

        fail_ratio = 100.0
        total_cnt = wrong_cnt + empty_cnt + succ_cnt
        if total_cnt != 0:
            fail_ratio = float(wrong_cnt + empty_cnt) / total_cnt
        logging.info("uniq_res: line_cnt={}, succ_cnt={}, wrong_cnt={}, empty_cnt={}, fail_ratio={}".format(line_cnt, succ_cnt, wrong_cnt, empty_cnt, fail_ratio))
        self.doc_dict = doc_dict
        return fail_ratio

    def writeRes(self, input_filename, output_filename):
        wrong_cnt = 0
        empty_cnt = 0
        succ_cnt = 0
        line_cnt = 0
        with open(input_filename, 'r') as in_f, open(output_filename, 'w') as out_f:
            for line in in_f.readlines():
                line_cnt += 1
                line = line.strip()
                elems = line.split("\t")
                if len(elems) != 10:
                    logging.info("wrong final:{}".format(line))
                    wrong_cnt += 1
                    continue
                userid = elems[0]
                docid = elems[1]

                if docid not in self.doc_dict:
                    logging.info("empty final:{}".format(line))
                    empty_cnt += 1
                    status = 'not_reviewed'
                    reason = ''
                else:
                    (status, reason) = self.doc_dict[docid]
                    succ_cnt += 1
                out_f.write("{}\t{}\t{}\n".format(line, status, reason))

        fail_ratio = 100.0
        total_cnt = wrong_cnt + empty_cnt + succ_cnt
        if total_cnt != 0:
            fail_ratio = float(wrong_cnt + empty_cnt) / total_cnt
        logging.info("final_res: line_cnt={}, succ_cnt={}, wrong_cnt={}, empty_cnt={}, fail_ratio={}".format(line_cnt, succ_cnt, wrong_cnt, empty_cnt, fail_ratio))
        return fail_ratio


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("read_review.py input_file output_file log_file")
        sys.exit(1)

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    log_filename = sys.argv[3]

    if not os.path.exists(input_filename):
        print('error: file_not_found', input_filename)
        sys.exit(0)

    logging.basicConfig(filename=log_filename, filemode="w", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    read_manager = ReadManager()
    fail_ratio = read_manager.readDict(input_filename)

    if fail_ratio > 0.8:
        sys.exit(0)

    fail_ratio = read_manager.writeRes(input_filename, output_filename)
    if fail_ratio > 0.8:
        sys.exit(0)
