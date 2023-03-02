#!/bin/python
import sys
import json
import time

class Obj:
    def __init__(self, doc_id, pos, context_meta):
        self.doc_id = doc_id
        self.pos = pos
        self.is_local = self.isLocal(context_meta)
        self.condition = self.getCondition(context_meta)

    def isLocal(self, context_meta):
        condition_key = "condition"
        if condition_key not in context_meta:
            return False
        condition = context_meta[condition_key].encode("utf-8", "ignore")
        if condition.startswith("local"):
            return True
        return False

    def getCondition(self, context_meta):
        condition_key = "condition"
        if condition_key not in context_meta:
            return "null"
        condition = context_meta[condition_key].encode("utf-8", "ignore")
        return condition

    def objStr(self):
        feature_list = [self.doc_id, self.pos, self.is_local, self.condition]
        return "\t".join([str(i) for i in feature_list])


class ParseForYou:
    def __init__(self, is_local, uid, input_html):
        self.obj_list = []
        self.is_local = is_local
        self.uid = uid
        self.input_html = input_html

    def parseJson(self):
        f = open(self.input_html, 'r')
        line = f.readline()
        json_obj = json.loads(line)

        self.obj_list = []
        for obj in json_obj["documents"]:
            doc_id = obj["docid"].encode("utf-8", "ignore")
            pos = obj["pos"]
            context_meta = obj["contextMeta"]
            parse_obj = Obj(doc_id, pos, context_meta)
            self.obj_list.append(parse_obj)

    def parse(self):
        self.parseJson()
        for obj in self.obj_list:
            if self.is_local or (self.is_local == False and obj.is_local == False):
                print(self.uid + "\t" + obj.objStr())

def parseUrlInfo(curr_date, url_html):
    f = open(url_html, 'r')
    line = f.readline()
    json_obj = json.loads(line)

    data_key = "data"
    static_key = "static_feature"

    if data_key not in json_obj:
        return 1

    keys = json_obj[data_key].keys()
    if len(keys) != 1:
        return 1

    sub_obj = json_obj[data_key][keys[0]]
    if static_key not in sub_obj:
        return 1

    url = sub_obj[static_key]["url"].encode("utf-8", "ignore")
    domain = sub_obj[static_key]["domain"].encode("utf-8", "ignore")
    title = sub_obj[static_key]["stitle"].replace("\t", " ").encode("utf-8", "ignore")
    pub_time = sub_obj[static_key]["date"].encode("utf-8", "ignore")
    res_list = [url, domain, title, pub_time, curr_date]
    print("\t".join([str(i) for i in res_list]))
    return 0

def uniqForAssess(max_assess_num, input_file):
    in_f = open(input_file, 'r')
    docid_dict = {}
    for line in in_f.readlines():
        line = line.strip()
        elems = line.split("\t")
        if len(elems) < 2 or len(docid_dict) > max_assess_num:
            continue
        docid = elems[1]
        if docid not in docid_dict:
            print(line)
        docid_dict[docid] = 1

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("proc.py arguments[parse_foryou/parse_url_info/uniq_for_asess]")
        sys.exit(1)

    if sys.argv[1] == "parse_foryou":
        if len(sys.argv) != 5:
            print("proc.py parse_foryou is_local[0/1] uid input.html")
            sys.exit(1)

        # default local
        is_local = False
        if (sys.argv[2] == "1"):
            is_local = True

        uid = sys.argv[3]
        input_html = sys.argv[4]
        obj = ParseForYou(is_local, uid, input_html)
        obj.parse()

    elif sys.argv[1] == "parse_url_info":
        if len(sys.argv) != 4:
            print("null")
            sys.exit(0)
        curr_date = sys.argv[2]
        url_html = sys.argv[3]
        ret = parseUrlInfo(curr_date, url_html)
        if ret != 0:
            print("\t".join("null" for i in range(0, 5)))

    elif sys.argv[1] == "uniq_for_assess":
        if len(sys.argv) != 4:
            sys.exit(1)
        max_assess_num = int(sys.argv[2])
        input_file = sys.argv[3]
        uniqForAssess(max_assess_num, input_file)(base)
