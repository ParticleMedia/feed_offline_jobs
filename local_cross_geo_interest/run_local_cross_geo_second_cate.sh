# !/bin/bash
set -x
set -e
set -u

##### runtime conf
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
PYTHON_BIN="/home/services/miniconda3/bin/python3"
TIME_TAG=`date +%Y%m%d%H`

##### task conf
TASK_NAME="local_cross_geo_second_cat"

HDFS_PATH="s3a://pm-hdfs2/user/tangyuyang/${TASK_NAME}"
LOCAL_DATA="/data/tangyuyang/${TASK_NAME}/data"
LOCAL_LOG="/data/tangyuyang/${TASK_NAME}/log"

# redis
REDIS_PREFIX="local_geo_second_cat"
REDIS_TTL_SEC=86400

# sql
CJV_DATE_DIFF=3
DOC_DATE_DIFF=3
CJV_SDATE=`date +%Y-%m-%d -d "-${CJV_DATE_DIFF} days"`
DOC_SDATE=`date +%Y-%m-%d -d "-${DOC_DATE_DIFF} days"`
ZIP_PDATE=`date +%Y-%m-%d -d "-2 days"`
MIN_ZIP_CATE_USER=1000
MIN_CHECK=100

CLEAR_DAY=5
MIN_RESULT_NUM=100


function get_docs() {
    local hdfs_cjv_path=${HDFS_PATH}"/data_${TIME_TAG}"
    local merged_file=${LOCAL_DATA}/${TIME_TAG}_merged
    local kv_file=${LOCAL_DATA}/${TIME_TAG}_kv
    local hive_sql="
WITH cate_geo_doc AS (
  SELECT
    doc.doc_id,
    second_cat,
    concat(exid.type, '@', exid.pid) as type_pid
  FROM dim.document_parquet doc
  LATERAL VIEW explode(geotag) idtable as exid
  LATERAL VIEW explode(text_category.second_cat) tmpTable AS second_cat, second_cat_score
  WHERE doc.pdate >= '${DOC_SDATE}'
    AND exid.type != 'state'
)

, cate_doc AS (
  SELECT
    doc.doc_id,
    second_cat
  FROM dim.document_parquet doc
  LATERAL VIEW explode(text_category.second_cat) tmpTable AS second_cat, second_cat_score
  WHERE doc.pdate >= '${DOC_SDATE}'
)

, t_zip AS (
  SELECT
    id,
    city_id
  FROM dim.location_zipcode_pst_daily_ss
  WHERE pdate = '${ZIP_PDATE}'
)

, zip_cate_cjv AS (
  SELECT
    city_id,
    second_cat,

    round(1.0000 * sum(cjv.clicked) / sum(cjv.checked), 4) as ctr,
    sum(cjv.checked) as check,
    sum(cjv.clicked) as click,
    count(distinct cjv.doc_id) as docs,
    count(distinct cjv.user_id) as users
  FROM warehouse.online_cjv_hourly cjv
  LEFT JOIN cate_doc ON cate_doc.doc_id = cjv.doc_id
  LEFT JOIN t_zip ON t_zip.id = cjv.nr_zip
  WHERE
    cjv.joined = 1
    AND cjv.pdate >= '${CJV_SDATE}'
    AND cjv.channel_name in ('foryou', 'local')
    AND (cjv.nr_condition rlike 'local')
    AND cjv.checked = 1
    AND city_id IS NOT NULL
  GROUP BY city_id,second_cat
)

insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by '\t'

SELECT
  t.city_id,
  t.second_cat,
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
      city_id,
      second_cat,
      type_pid,

      round(1.0000 * sum(cjv.clicked) / sum(cjv.checked), 4) as ctr,
      sum(cjv.checked) as check,
      sum(cjv.clicked) as click,
      count(distinct cjv.doc_id) as docs,
      count(distinct cjv.user_id) as users
    FROM warehouse.online_cjv_hourly cjv
    LEFT JOIN cate_geo_doc ON cate_geo_doc.doc_id = cjv.doc_id
    LEFT JOIN t_zip ON t_zip.id = cjv.nr_zip
    WHERE
      cjv.joined = 1
      AND cjv.pdate >= '${CJV_SDATE}'
      AND cjv.channel_name in ('foryou', 'local')
      AND (cjv.nr_condition rlike 'local')
      AND cjv.checked = 1
      AND city_id IS NOT NULL
    GROUP BY city_id,second_cat,type_pid
) t
LEFT JOIN zip_cate_cjv ON t.city_id = zip_cate_cjv.city_id AND t.second_cat = zip_cate_cjv.second_cat
WHERE zip_cate_cjv.users >= ${MIN_ZIP_CATE_USER}
  AND t.ctr > zip_cate_cjv.ctr
  AND t.check >= ${MIN_CHECK}
ORDER BY t.city_id,t.second_cat,t.ctr DESC
LIMIT 1000000
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
    echo "merged_file=${merged_file} result_cnt=${res_cnt}"
    if [ $res_cnt -lt ${MIN_RESULT_NUM} ]; then
        echo "result count is less than ${MIN_RESULT_NUM}, please check"
        exit 1
    fi

    echo "process_to_kv_res"
    ${PYTHON_BIN} ./process_to_kv_res.py \
        --input ${merged_file} \
        --output ${kv_file}

    echo "start write to redis"
    ${PYTHON_BIN} ./write_redis.py \
        --input ${kv_file} \
        --prefix ${REDIS_PREFIX} \
        --ttl ${REDIS_TTL_SEC}
    return $?
}

function clear_old_data() {
    # remove out-of-date data
    echo "clear"
    CJV_OLD_SDATE=`date +%Y-%m-%d -d "-${CLEAR_DAY} days"`

    rm -rf ${LOCAL_DATA}"/${CJV_OLD_SDATE}*"
    rm -rf ${LOCAL_DATA}/.*.crc
    rm -rf ${LOCAL_LOG}"/${CJV_OLD_SDATE}*"
}


mkdir -p ${LOCAL_DATA}
mkdir -p ${LOCAL_LOG}

log_file=${LOCAL_LOG}"/${TIME_TAG}.log"
get_docs | tee -a ${log_file}

clear_old_data
