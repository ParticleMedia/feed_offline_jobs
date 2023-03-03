#!/usr/bin/env python
# -*- coding:utf-8 -*-

from rediscluster import RedisCluster
from pyhive import hive
import datetime
import argparse
import logging
import zipimport
import pymongo
import bson
import pytz
import json
import socket
import time
import requests


def write_metric(metric, ts, val, host):
    payload = {
        "metric": metric,
        "timestamp": ts,
        "value": val,
        "tags": {
            "host": host
        }
    }
    s = requests.Session()
    r = s.post("http://opentsdb.ha.nb.com:4242/api/put?details", json=[payload])

class EngageInfo:
    def __init__(self):
        self.docId = ""
        self.likes = 0
        self.cmts = 0

    def toString(self):
        return "{"+"docId:{},likes:{},cmts:{}"\
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
    return engageList[:300]

def sortByLikes(engageList):
    engageList = sorted(engageList, key=lambda engage: -engage.likes)
    return engageList[:300]

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

def queryCtrCommandTot(startDate,endDate):

    command = """
    select
    a.os,count(distinct a.user_id) as `dau`,sum(a.clicked),sum(a.checked),sum(a.shared),sum(a.clicked) / sum(a.checked) as `ctr`,sum(a.shared) / sum(a.clicked) as `share/click`,sum(a.shared) / sum(a.checked) as `share/check`,sum(a.clicked) / count(distinct a.user_id) as `click_pu`,sum(a.checked) / count(distinct a.user_id) as `check_pu`,sum(a.shared)/ count(distinct a.user_id) as `share_pu`,sum(a.thumbed_up)/count(distinct a.user_id) as `up_pu` from
(select
    os,
    doc_id,user_id,
    case
    when channel_id = 'k122516' then 'crime'
    when channel_id = 'k122515' then 'entertain'
    when channel_id = 'k122521' then 'sport'
    when channel_id = 'k122522' then 'lifestyle'
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
    when channel_id = 'k122566' then 'home_garden'
    when channel_id = 'k122571' then 'animal'
    when channel_id = 'k122572' then 'military'
    when channel_id = 'k122573' then 'games'
    when channel_id = 'k122574' then 'comics'
    when channel_id = 'k122575' then 'horoscope'
    end as `tab`,
    clicked,
    checked,
    shared,
    thumbed_up
from warehouse.online_cjv_parquet_hourly where
pdate >= '{0}' and pdate <= '{1}' and
channel_name = "channelnew" and
joined = true and
checked = true) a group by a.os
    """.format(startDate, startDate)
    print(command)
    d = datetime.datetime.strptime(startDate, "%Y-%m-%d")
    t = d.timetuple()
    timeStamp = int(time.mktime(t))
    hostname = socket.gethostname()
    dataList = call_hue(command,'impala')
    for item in dataList:
        try:
            os = item[0]
            dau = float(item[1])
            clicks = float(item[2])
            checks = float(item[3])
            shares = float(item[4])
            ctr = float(item[5])
            str1 = float(item[6])
            str2 = float(item[7])
            click_pu = float(item[8])
            check_pu = float(item[9])
            share_pu = float(item[10])
            up_pu = float(item[11])
            tabName = 'tot'
            write_metric("tabmetric.dau."+os+"."+tabName, timeStamp, dau, hostname)
            write_metric("tabmetric.clicks."+os+"."+tabName, timeStamp, clicks, hostname)
            write_metric("tabmetric.checks."+os+"."+tabName, timeStamp, checks, hostname)
            write_metric("tabmetric.shares."+os+"."+tabName, timeStamp, shares, hostname)
            write_metric("tabmetric.ctr."+os+"."+tabName, timeStamp, ctr, hostname)
            write_metric("tabmetric.str1."+os+"."+tabName, timeStamp, str1, hostname)
            write_metric("tabmetric.str2."+os+"."+tabName, timeStamp, str2, hostname)
            write_metric("tabmetric.click_pu."+os+"."+tabName, timeStamp, click_pu, hostname)
            write_metric("tabmetric.check_pu."+os+"."+tabName, timeStamp, check_pu, hostname)
            write_metric("tabmetric.share_pu."+os+"."+tabName, timeStamp, share_pu, hostname)
            write_metric("tabmetric.up_pu."+os+"."+tabName, timeStamp, up_pu, hostname)
            print (item)
        except Exception as e:
            logging.error("exception when format data %s %s", item,e.message)
    print("query Done")

def queryCtrCommand(startDate,endDate):

    command = """
    select
    a.tab,a.os,count(distinct a.user_id) as `dau`,sum(a.clicked),sum(a.checked),sum(a.shared),sum(a.clicked) / sum(a.checked) as `ctr`,sum(a.shared) / sum(a.clicked) as `share/click`,sum(a.shared) / sum(a.checked) as `share/check`,sum(a.clicked) / count(distinct a.user_id) as `click_pu`,sum(a.checked) / count(distinct a.user_id) as `check_pu`,sum(a.shared)/ count(distinct a.user_id) as `share_pu`,sum(a.thumbed_up)/count(distinct a.user_id) as `up_pu` from
(select
    os,
    doc_id,user_id,
    case
    when channel_id = 'k122516' then 'crime'
    when channel_id = 'k122515' then 'entertain'
    when channel_id = 'k122521' then 'sport'
    when channel_id = 'k122522' then 'lifestyle'
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
    when channel_id = 'k122566' then 'home_garden'
    when channel_id = 'k122571' then 'animal'
    when channel_id = 'k122572' then 'military'
    when channel_id = 'k122573' then 'games'
    when channel_id = 'k122574' then 'comics'
    when channel_id = 'k122575' then 'horoscope'
    when channel_id = 'k122561' then 'outdoor'
    when channel_id = 'k122586' then 'finance'
    when channel_id = 'k26164' then 'headline'
    end as `tab`,
    clicked,
    checked,
    shared,
    thumbed_up
from warehouse.online_cjv_parquet_hourly where
pdate >= '{0}' and pdate <= '{1}' and
(channel_name = "channelnew" or (channel_name = "channel" and channel_id = "k26164")) and
joined = true and
checked = true) a group by a.tab,a.os
    """.format(startDate, startDate)
    print(command)
    d = datetime.datetime.strptime(startDate, "%Y-%m-%d")
    t = d.timetuple()
    timeStamp = int(time.mktime(t))
    hostname = socket.gethostname()
    dataList = call_hue(command,'impala')
    for item in dataList:
        try:
            tabName = item[0]
            os = item[1]
            dau = float(item[2])
            clicks = float(item[3])
            checks = float(item[4])
            shares = float(item[5])
            ctr = float(item[6])
            str1 = float(item[7])
            str2 = float(item[8])
            click_pu = float(item[9])
            check_pu = float(item[10])
            share_pu = float(item[11])
            up_pu = float(item[12])
            if tabName is None:
                continue
            write_metric("tabmetric.dau."+os+"."+tabName, timeStamp, dau, hostname)
            write_metric("tabmetric.clicks."+os+"."+tabName, timeStamp, clicks, hostname)
            write_metric("tabmetric.checks."+os+"."+tabName, timeStamp, checks, hostname)
            write_metric("tabmetric.shares."+os+"."+tabName, timeStamp, shares, hostname)
            write_metric("tabmetric.ctr."+os+"."+tabName, timeStamp, ctr, hostname)
            write_metric("tabmetric.str1."+os+"."+tabName, timeStamp, str1, hostname)
            write_metric("tabmetric.str2."+os+"."+tabName, timeStamp, str2, hostname)
            write_metric("tabmetric.click_pu."+os+"."+tabName, timeStamp, click_pu, hostname)
            write_metric("tabmetric.check_pu."+os+"."+tabName, timeStamp, check_pu, hostname)
            write_metric("tabmetric.share_pu."+os+"."+tabName, timeStamp, share_pu, hostname)
            write_metric("tabmetric.up_pu."+os+"."+tabName, timeStamp, up_pu, hostname)
            print (item)
        except Exception as e:
            logging.error("exception when format data %s %s", item,e.message)
    print("query Done")

if __name__ == "__main__":
    global redisClient
    delta = datetime.timedelta(days=1)

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
    queryCtrCommandTot(startTime,endTime)
