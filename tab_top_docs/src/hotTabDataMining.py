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
import time
import requests

class EngageInfo:
    def __init__(self):
        self.tabId = ""
        self.docId = ""
        self.ctr = 0
        self.check = 0
        self.click = 0
        self.tabName = ""
        self.tab1d = {}
        self.tab2d = {}

    def accCtr(self):
        tab1dcheck = float(self.tab1d.get('view',0))
        tab1dclick = float(self.tab1d.get('click',0))
        tab2dcheck = float(self.tab2d.get('view',0))
        tab2dclick = float(self.tab2d.get('click',0))
        totcheck = tab1dcheck + tab2dcheck * 0.5 + 30
        totclick = tab1dclick + tab1dclick * 0.5 + 1
        return totclick / totcheck

    def accCheck(self):
        tab1dcheck = float(self.tab1d.get('view',0))
        tab2dcheck = float(self.tab2d.get('view',0))
        totcheck = tab1dcheck + tab2dcheck * 0.5 + 30
        return totcheck


    def accClick(self):
        tab1dclick = float(self.tab1d.get('click',0))
        tab2dclick = float(self.tab2d.get('click',0))
        totclick = tab1dclick + tab1dclick * 0.5 + 1
        return totclick

    def toString(self):
        return "{"+"\"docId\":\"{}\",\"tabId\":\"{}\",\"ctr\":{},\"click\":{},\"check\":{}"\
            .format(self.docId,self.tabId,self.accCtr(),self.accClick(),self.accCheck())+"}"

tabTopDict = {}
docTs = {}
shortExpireTab = ['k122521','k122525']

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

def sortByCtr(engageList):
    engageList = sorted(engageList, key=lambda engage: -engage.ctr)
    return engageList

def sortByCtrV2(engageList):
    engageList = sorted(engageList, key=lambda engage: -engage.accCtr())
    return engageList


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

def filterDoc7dAgo(tabList):
    now = time.time()
    res = []
    for info in tabList:
        ts = docTs.get(info.docId,0)
        if now - ts > 3600 * 24 * 7:
            continue
        if info.tabId in shortExpireTab:
            if now - ts > 3600 * 24 * 2:
                continue
        res.append(info)
    return res

def rerankLowCheck(tabList):
    t1 = []
    t2 = []
    for info in tabList:
        if info.check >= 60:
            t1.append(info)
        else:
            t2.append(info)
    t1.extend(t2)
    return t1

def getDocProfiles(tabList):
    docIds = []
    profile = {}
    for entity in tabList:
        docIds.append(entity.docId)
    step = 100
    for ind in range(0,len(docIds),step):
        url = 'http://doc-profile.ha.nb.com:9600/get?from=tabtopdoc&fields=tab_cfb_1d.*,tab_cfb_2d.*'
        step_ids = docIds[ind : ind + step]
        docStr = ','.join(step_ids)
        url = url + '&docids=' + docStr
        print(url)
        r = requests.get(url)
        js = json.loads(r.content)
        if js.has_key("data"):
            for (docId,value) in js["data"].items():
                profile[docId] = value
    for entity in tabList:
        if profile.has_key(entity.docId):
            if profile[entity.docId].has_key('tab_cfb_1d'):
                info = profile[entity.docId]['tab_cfb_1d']['data']
                if info.has_key(entity.tabId):
                    entity.tab1d = info[entity.tabId]
            if profile[entity.docId].has_key('tab_cfb_2d'):
                info = profile[entity.docId]['tab_cfb_2d']['data']
                if info.has_key(entity.tabId):
                    entity.tab2d = info[entity.tabId]
    return tabList


def queryCtrCommand(startDate,endDate):

    command = """
    select
    a.tab,a.doc_id,a.channel_id,sum(a.clicked) / sum(a.checked) as `ctr`,sum(a.checked) as `check`,sum(a.clicked) as `click`  from
(select
    doc_id,user_id,channel_id,
    case
    when channel_id = 'k122516' then 'crime'
    when channel_id = 'k122515' then 'entertain(celebrity,tv,movie,music)'
    when channel_id = 'k122521' then 'sport(nfl,nba,mlb,nhl,wwe)'
    when channel_id = 'k122522' then 'lifestyle(food,travel)'
    when channel_id = 'k122551' then 'pet'
    when channel_id = 'k122524' then 'society'
    when channel_id = 'k122525' then 'tech'
    when channel_id = 'k122519' then 'health'
    when channel_id = 'k122552' then 'science'
    when channel_id = 'k122564' then 'vehicles'
    when channel_id = 'k122567' then 'food'
    when channel_id = 'k122565' then 'travel'
    when channel_id = 'k122563' then 'fitness'
    when channel_id = 'k122562' then 'beauty'
    when channel_id = 'k122571' then 'animal'
    when channel_id = 'k122572' then 'military'
    when channel_id = 'k122573' then 'games'
    when channel_id = 'k122574' then 'comics'
    when channel_id = 'k122575' then 'horoscope'
    end as `tab`,
    clicked,
    checked
from warehouse.online_cjv_hourly where
pdate >= '{0}' and pdate <= '{1}' and
channel_name = "channelnew" and
joined = true and
checked = true) a
group by a.tab,a.doc_id,a.channel_id
    """.format(startDate, endDate)

    print (command)
    dataList = call_hue(command,'impala')
    docIdList = []
    docCtrDict = {}
    for item in dataList:
        try:
            tabName = item[0]
            docId = item[1]
            tabId = item[2]
            ctr = item[3]
            check = item[4]
            click = item[5]
            docIdList.append(docId)
            info = EngageInfo()
            info.tabName = tabName
            info.tabId = tabId
            info.docId = docId
            info.ctr = float(ctr)
            info.check = float(check)
            info.click = float(click)
            if not tabTopDict.has_key(tabId):
                tabTopDict[tabId] = []
            tabTopDict[tabId].append(info)
            docIdList.append(docId)
        except Exception as e:
            logging.error("exception when format data %s %s", item,e.message)
    print("query Done")
    docs = collect.find({"_id": {"$in":docIdList}})
    for doc in docs:
        docId = doc.get("_id", "")
        ts = doc.get("epoch",0)
        docTs[docId] = ts
    for tabId in tabTopDict:
        #tabList = filterDoc7dAgo(tabTopDict[tabId])
        tabList = getDocProfiles(tabTopDict[tabId])
        tabList = sortByCtrV2(tabList)
        tabList = rerankLowCheck(tabList)
        key = "tabHotDocs#2d"
        store2Redis(tabId,key,tabList)
        for item in tabList:
            print item.toString()
            print (time.time() - docTs.get(item.docId,0)) / (3600.0 * 24.0)
        print "\n"


if __name__ == "__main__":
    global redisClient
    delta = datetime.timedelta(days=2)

    utc_tz = pytz.timezone('UTC')
    edate = datetime.datetime.now(tz=utc_tz)

    sdate = edate - delta
    startTime = datetime.datetime.strftime(sdate, "%Y-%m-%d")
    endTime = datetime.datetime.strftime(edate, "%Y-%m-%d")

    client = pymongo.mongo_client.MongoClient("172.31.24.51,172.31.29.170,172.31.24.237")
    collect = client.staticFeature.document

    redis_basis_conn = [{'host': '172.31.24.73', 'port': 6379}, {'host': '172.31.20.144', 'port': 6379},
                        {'host': '172.31.27.131', 'port': 6379},{'host': '172.31.16.27', 'port': 6379},
                        {'host': '172.31.29.198', 'port': 6379},{'host': '172.31.18.166', 'port': 6379},
                        {'host': '172.31.27.19', 'port': 6379},{'host': '172.31.20.211', 'port': 6379}]
    redisClient = RedisCluster(startup_nodes=redis_basis_conn, decode_responses=True)
    queryCtrCommand(startTime,endTime)
