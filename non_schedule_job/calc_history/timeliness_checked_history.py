#!/usr/bin/env python3

from livy.models import SessionKind
from livy.session import LivySession

import pandas as pd

if __name__ == "__main__":
    # livy_url = 'http://lqs.emr.nb.com:8998'
    livy_url = 'http://receng.emr.nb.com:8998'
    mem = '12g'

    cjv_begin_date = "2022-07-01"
    cjv_end_date = "2022-08-01"
    doc_begin_date = "2022-02-01"
    doc_end_date = "2022-08-10"
    dim_date = "2022-08-01"

    job_name = f"timeliness_check_history_{cjv_begin_date}_{cjv_end_date}"
    hive_path = f"s3://pm-hdfs2/user/tangyuyang/timeliness_longterm_check_history/{cjv_begin_date}_{cjv_end_date}"

    print(f"start run beg={cjv_begin_date} end={cjv_end_date} on dim_date={dim_date}")
    print(f'job_name={job_name}')
    print(f'hive_path={hive_path}')

    hive_sql = f"""
WITH docs AS (
SELECT
  doc_id,
  publish_time,
  if(timeliness_type = 'current' OR timeliness_type = 'evergreen', 1, 0) as is_longterm,
  if(is_news_score < 0.5, 1, 0) as is_nonnews
FROM ods.ods_mongo_static_feature_document_pst_daily_ss doc
WHERE pdate = '{dim_date}'
    AND date(publish_time) between '{doc_begin_date}' and '{doc_end_date}'
)

insert overwrite directory '{hive_path}' row format delimited fields terminated by ','

SELECT /*+ REPARTITION(100) */
  cjv.user_id,
  CONCAT_WS('\t', COLLECT_SET(DISTINCT(CONCAT(cjv.doc_id, '|', CAST(cjv.ts AS string))))) AS doc_items
FROM warehouse.online_cjv_hourly cjv
LEFT JOIN docs ON cjv.doc_id = docs.doc_id
WHERE cjv.joined = 1
  AND cjv.pdate BETWEEN '{cjv_begin_date}' AND '{cjv_end_date}'
  AND cjv.checked = 1
  AND cjv.cv_time >= 2000
  AND cjv.channel_name in ('foryou', 'local')
  AND cjv.user_id > 0
  AND (docs.is_longterm = 1 OR docs.is_nonnews = 1 OR (cjv.nr_condition rlike 'evergreen'))
  AND date(publish_time) between '{doc_begin_date}' and '{doc_end_date}'
GROUP BY cjv.user_id
    """

    spark_conf = {"livy.rsc.server.connect.timeout": "86400s"}
    with LivySession.create(livy_url, kind=SessionKind.SQL, driver_memory=mem, name=job_name, spark_conf=spark_conf) as session:
        print('hive_sql\n' + hive_sql)
        df = session.run(hive_sql)
        print('finish run')
