# !/bin/bash


##### runtime conf
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
PYTHON_BIN="/home/services/miniconda3/bin/python3"
TODAY=`date +%Y-%m-%d`
HOUR=`date +%H`
TIME_TAG=`date +%Y%m%d%H`

##### task conf
TASK_NAME="local_cross_geo_interest"

HDFS_PATH="s3a://pm-hdfs2/user/tangyuyang/local_cross_geo_interest"
LOCAL_DATA="/data/tangyuyang/local_cross_geo_interest/data"
LOCAL_LOG="/data/tangyuyang/local_cross_geo_interest/log"

CLEAR_DAY=30
MIN_RESULT_NUM=100

# sql
CJV_DATE_DIFF=2
DOC_DATE_DIFF=2
CJV_SDATE=`date +%Y-%m-%d -d "-${CJV_DATE_DIFF} days"`
DOC_SDATE=`date +%Y-%m-%d -d "-${DOC_DATE_DIFF} days"`
MIN_ZIP_CATE_USER=1000
MIN_ZIP_CATE_GEO_USER=100
MIN_CTR_DIFF=0.03
MIN_CHECK=100

# redis
REDIS_PREFIX="local_geo_cate@"
REDIS_TTL_SEC=86400


function get_docs() {
    local hdfs_cjv_path=${HDFS_PATH}"/data_${TIME_TAG}"
    local merged_file=${LOCAL_DATA}/${TIME_TAG}_data
    local hive_sql="
WITH cate_geo_doc AS (
  SELECT
    doc.doc_id,
    first_cat,
    concat(exid.type, '@', exid.pid) as type_pid
  FROM dim.document_parquet doc
  LATERAL VIEW explode(geotag) idtable as exid
  LATERAL VIEW explode(text_category.first_cat) tmpTable AS first_cat, first_cat_score
  WHERE doc.pdate >= '${DOC_SDATE}'
)

, cate_doc AS (
  SELECT
    doc.doc_id,
    first_cat
  FROM dim.document_parquet doc
  LATERAL VIEW explode(text_category.first_cat) tmpTable AS first_cat, first_cat_score
  WHERE doc.pdate >= '${DOC_SDATE}'
)

, zip_cate_cjv AS (
  SELECT
    nr_zip,
    first_cat,

    1.0000 * sum(cjv.clicked) / sum(cjv.checked) as ctr,
    sum(cjv.checked) as check,
    sum(cjv.clicked) as click,
    count(distinct cjv.doc_id) as docs,
    count(distinct cjv.user_id) as users
  FROM warehouse.online_cjv_hourly cjv
  LEFT JOIN cate_doc ON cate_doc.doc_id = cjv.doc_id
  WHERE
    cjv.joined = 1
    AND cjv.pdate >= '${CJV_SDATE}'
    AND cjv.channel_name in ('foryou', 'local')
    AND (cjv.nr_condition rlike 'local')
    AND cjv.checked = 1
    AND cjv.nr_zip IS NOT NULL
  GROUP BY nr_zip,first_cat
)

SELECT
  t.nr_zip,
  t.first_cat,
  t.type_pid,

  t.ctr,
  t.check,
  t.click,
  t.docs,
  t.users,
  zip_cate_cjv.ctr as zipcat_ctr,
  zip_cate_cjv.check as zipcat_check,
  zip_cate_cjv.click as zipcat_click,
  zip_cate_cjv.docs as zipcat_docs,
  zip_cate_cjv.users as zipcat_users
FROM (
    SELECT
      nr_zip,
      first_cat,
      type_pid,

      1.0000 * sum(cjv.clicked) / sum(cjv.checked) as ctr,
      sum(cjv.checked) as check,
      sum(cjv.clicked) as click,
      count(distinct cjv.doc_id) as docs,
      count(distinct cjv.user_id) as users
    FROM warehouse.online_cjv_hourly cjv
    LEFT JOIN cate_geo_doc ON cate_geo_doc.doc_id = cjv.doc_id
    WHERE
      cjv.joined = 1
      AND cjv.pdate >= '${CJV_SDATE}'
      AND cjv.channel_name in ('foryou', 'local')
      AND (cjv.nr_condition rlike 'local')
      AND cjv.checked = 1
      AND cjv.nr_zip IS NOT NULL
    GROUP BY nr_zip,first_cat,type_pid
) t
LEFT JOIN zip_cate_cjv ON t.nr_zip = zip_cate_cjv.nr_zip AND t.first_cat = zip_cate_cjv.first_cat
WHERE zip_cate_cjv.users >= ${MIN_ZIP_CATE_USER}
  AND t.users >= ${MIN_ZIP_CATE_GEO_USER}
  AND t.ctr >= zip_cate_cjv.ctr + ${MIN_CTR_DIFF}
  AND t.check >= ${MIN_CHECK}
ORDER BY t.nr_zip,t.first_cat,t.ctr DESC
LIMIT 100000
    "

    local sql_file=${LOCAL_LOG}"/${TIME_TAG}.sql"
    echo "${hive_sql}" > ${sql_file}

    echo "start run sql"
    ${HIVE_BIN} \
        --hiveconf mapreduce.job.name=${TASK_NAME} \
        --hiveconf mapreduce.job.queuename=profile \
        --hiveconf yarn.app.mapreduce.am.resource.mb=8192 \
        --hiveconf tez.am.resource.memory.mb=8192 \
        --hiveconf mapreduce.map.memory.mb=4096 \
        --hiveconf mapreduce.reduce.memory.mb=4096 \
        -f ${sql_file}

    echo "start download merged data"
    hadoop fs -getmerge ${hdfs_cjv_path}/* ${merged_file}

    local res_cnt=`cat ${merged_file} | wc -l`
    if [ res_cnt -lt ${MIN_RESULT_NUM} ]; then
        echo "result count is less than ${MIN_RESULT_NUM}, please check"
        exit 1
    fi

    echo "start write to redis"
    ${PYTHON_BIN} ./write_redis.py \
        --input ${merged_file} \
        --prefix ${REDIS_PREFIX} \
        --ttl ${REDIS_TTL_SEC}
    return $?
}

function clear_old_data() {
    # remove out-of-date data
    echo "clear"
#    find /data/qihengda/local_supplement/data/ -mtime +${CLEAR_DAY} -name 'docs*' -exec rm -rf {} \;
#    find /data/qihengda/local_supplement/ -mtime +${CLEAR_DAY} -name 'sql.*' -exec rm -rf {} \;

    CJV_SDATE=`date +%Y-%m-%d -d "-${CJV_DATE_DIFF} days"`

    rm -rf ${LOCAL_DATA}"/"
    rm -rf ${LOCAL_DATA}"/*.crc"
}


mkdir -p ${LOCAL_DATA}
mkdir -p ${LOCAL_LOG}

log_file=${LOCAL_LOG}"/${TIME_TAG}.log"
get_docs > ${log_file}

clear_old_data