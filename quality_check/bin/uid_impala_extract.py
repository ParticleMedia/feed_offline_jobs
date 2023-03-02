#!/bin/python
import time
import sys
import logging
from pyhive import hive

def get_connection():
    conn = hive.Connection(host='receng.emr.nb.com', port=10000, username="hadoop")
    return conn

def hive_init(cursor):
    cursor.execute("set mapreduce.job.queuename=root.offline")

def execute_hue(job_name, sql):
    conn = get_connection()
    cur = conn.cursor()
    hive_init(cur)
    cur.execute("set mapreduce.job.name=" + job_name)
    cur.execute(sql)
    docs = []
    for doc in cur:
        docs.append(doc)
    conn.close()
    return docs

def execute_hue_with_retry(job_name, sql, retry):
    i = 0
    while True:
        i += 1
        if i > retry:
            break
        try:
            docs = execute_hue(job_name, sql)
            return docs
        except BaseException as e:
            logging.error("e:{}".format(e))
            time.sleep(10)
    return None


def main(pdate, uid_num):
    userid_list = []
    sql = '''
        select cjv.user_id, cjv.nr_zip
        from warehouse.online_cjv_hourly as cjv
        where cjv.pdate = "{}"
        and cjv.channel_name = "foryou"
        group by cjv.user_id, cjv.nr_zip
        order by rand()
        limit {}
    '''

    sql = sql.format(pdate, uid_num)
    try:
        docs = execute_hue_with_retry("tangyuyang_quality_check_uid_extract", sql, 10)
        for doc in docs:
            userid_list.append(str(doc[0]) + '\t' + str(doc[1]))
    except BaseException as e:
        print ("error")
        print (e)
        return 1
    for userid in userid_list:
        print(userid)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)
    pdate = sys.argv[1]
    uid_num = sys.argv[2]
    main(pdate, uid_num)(base)
