#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pyhive import hive
from rediscluster import RedisCluster
import datetime
import argparse
import logging
import zipimport
import pymongo
import bson
import pytz
import json

class EngageInfo:
    def __init__(self):
        self.docId = ""
        self.likes = 0
        self.cmts = 0

    def toString(self):
        return "{"+"\"docId\":\"{}\",\"likes\":{},\"cmts\":{}"\
            .format(self.docId,self.likes,self.cmts)+"}"

catTopDict = {}

def getAllCat(cat):
    catTag = []
    if cat is None or cat == "":
        return catTag
    if cat.has_key("first_cat"):
        for (key,value) in cat["first_cat"].items():
            catTag.append(key)
    if cat.has_key("second_cat"):
        for (key,value) in cat["second_cat"].items():
            catTag.append(key)
    if cat.has_key("third_cat"):
        for (key,value) in cat["third_cat"].items():
            catTag.append(key)
    return catTag

def sortByCmts(engageList):
    engageList = sorted(engageList, key=lambda engage: -engage.cmts)
    return engageList[:500]

def sortByLikes(engageList):
    engageList = sorted(engageList, key=lambda engage: -engage.likes)
    return engageList[:500]

def call_hue(command,method):
    global args
    conn = hive.Connection(host='receng.emr.nb.com', port=10000, username="hadoop")
    cursor = conn.cursor()
    cursor.execute("set mapreduce.job.queuename=root.offline")
    cursor.execute(command)
    data_list = cursor.fetchall()
    conn.close()
    return data_list

def store2Redis(catTag,key,entityList):
    global redisClient
    docs = []
    for entity in entityList:
        docs.append(entity.toString())
    if len(entityList) > 0:
        redisClient.hset(key,catTag,"["+','.join(docs)+"]")
    redisClient.expire(key,3600 * 24 * 30 * 6)
    return

def queryCtrCommand(startDate,endDate):

    command = """
    SELECT cjv.doc_id, max(doc_comment_cnt) as `cmt`,max(cjv.doc_thumb_up_cnt) as `likes`
    from warehouse.online_cjv_parquet_hourly cjv
    WHERE cjv.pdate >= '{0}' and cjv.pdate <= '{1}'
      and cjv.joined = 1
      and cjv.channel_name in ('foryou','local')
      and cjv.nr_condition like ('local%')
    GROUP BY cjv.doc_id
    """.format(startDate, endDate)

    print (command)
    dataList = call_hue(command,'impala')
    docIdList = []
    docCtrDict = {}
    for item in dataList:
        try:
            docId = item[0]
            cmts = item[1]
            likes = item[2]
            docIdList.append(docId)
            info = EngageInfo()
            info.docId = docId
            if cmts is None:
                info.cmts = 0
            else:
                info.cmts = float(cmts)
            if likes is None:
                info.likes = 0
            else:
                info.likes = float(likes)
            docCtrDict[docId] = info
        except Exception as e:
            logging.error("exception when format data %s %s", item,e.message)
    print("query Done")
    print(len(docIdList))
    docs = collect.find({"_id": {"$in":docIdList}})
    print("query static feature Done")
    docCnt = 0
    for doc in docs:
        docCnt = docCnt + 1
        if docCnt % 100 == 0:
            print docCnt
        docId = doc.get("_id", "")
        cat = doc.get("text_category_v2","")
        catList = getAllCat(cat)
        if docCtrDict.has_key(docId):
            for itemCat in catList:
                if not catTopDict.has_key(itemCat):
                    catTopDict[itemCat] = []
                docCtrDict[docId].catTag = catList
                catTopDict[itemCat].append(docCtrDict[docId])
    print "catNum:"
    print len(catTopDict)
    for catTag in catTopDict:
        catList = sortByCmts(catTopDict[catTag])
        key = "localTopCmts"
        store2Redis(catTag,key,catList)
        catList = sortByLikes(catTopDict[catTag])
        key = "localTopLikes"
        store2Redis(catTag, key, catList)
        print catTag
        for item in catList:
            print item.toString()
        print "\n"

if __name__ == "__main__":
    global redisClient
    delta = datetime.timedelta(days=2)

    utc_tz = pytz.timezone('UTC')
    edate = datetime.datetime.now(tz=utc_tz)

    sdate = edate - delta
    startTime = datetime.datetime.strftime(sdate, "%Y-%m-%d")
    endTime = datetime.datetime.strftime(edate, "%Y-%m-%d")

    client = pymongo.mongo_client.MongoClient("172.31.29.170,172.31.24.51,172.31.24.237")
    collect = client.staticFeature.document

    redis_basis_conn = [{'host': '172.31.24.73', 'port': 6379}, {'host': '172.31.20.144', 'port': 6379},
                        {'host': '172.31.27.131', 'port': 6379},{'host': '172.31.16.27', 'port': 6379},
                        {'host': '172.31.29.198', 'port': 6379},{'host': '172.31.18.166', 'port': 6379},
                        {'host': '172.31.27.19', 'port': 6379},{'host': '172.31.20.211', 'port': 6379}]
    redisClient = RedisCluster(startup_nodes=redis_basis_conn, decode_responses=True)
    queryCtrCommand(startTime,endTime)
