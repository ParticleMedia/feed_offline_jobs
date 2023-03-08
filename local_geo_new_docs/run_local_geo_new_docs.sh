# !/bin/bash
set -x
set -e
set -u

##### runtime conf
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
PYTHON_BIN="/home/services/miniconda3/bin/python3"
TIME_TAG=`date +%Y%m%d%H`

##### task conf
TASK_NAME="local_geo_new_docs"

HDFS_PATH="s3a://pm-hdfs2/user/tangyuyang/${TASK_NAME}"
LOCAL_DATA="/data/tangyuyang/${TASK_NAME}/data"
LOCAL_LOG="/data/tangyuyang/${TASK_NAME}/log"

# redis
REDIS_PREFIX="local_geo_new"
REDIS_TTL_SEC=86400

# sql
DOC_DATE_DIFF=1
DOC_HOUR_DIFF=6
DOC_SDATE=`date +"%Y-%m-%d" -d "-${DOC_DATE_DIFF} days"`
DOC_SHOUR=`date +"%Y-%m-%d %H:00:00" -d "-${DOC_HOUR_DIFF} hours"`
MAX_DOC_IN_GEO=50   # 单个key保存的结果数

CLEAR_DAY=30
MIN_RESULT_NUM=100


function get_docs() {
    local hdfs_cjv_path=${HDFS_PATH}"/data_${TIME_TAG}"
    local merged_file=${LOCAL_DATA}/${TIME_TAG}_data
    local hive_sql="
WITH geo_doc AS (
  SELECT
    first_cat,
    concat(exid.type, '@', exid.pid) as type_pid,
    concat(first_cat, '@', concat(exid.type, '@', exid.pid)) as cate_pid,
    doc.doc_id,
    doc.publish_time
  FROM dim.document_parquet doc
  LATERAL VIEW explode(text_category.first_cat) tmpTable AS first_cat, first_cat_score
  LATERAL VIEW explode(geotag) idtable as exid
  WHERE doc.pdate >= '${DOC_SDATE}'
    AND doc.publish_time >= '${DOC_SHOUR}'
    AND first_cat IS NOT NULL
    AND exid.pid IS NOT NULL
)

, order_doc AS (
  SELECT
    cate_pid,
    doc_id,
    publish_time,
    row_number() over(PARTITION BY cate_pid ORDER BY publish_time DESC) rk
  FROM geo_doc
)


insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by '\t'

SELECT
  cate_pid,
  concat_ws('#', collect_set(doc_id)) as docs
FROM order_doc
WHERE rk < ${MAX_DOC_IN_GEO}
  AND cate_pid IS NOT NULL
GROUP BY cate_pid
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
    CJV_OLD_SDATE=`date +%Y-%m-%d -d "-${CLEAR_DAY} days"`

    rm -rf ${LOCAL_DATA}"/${CJV_OLD_SDATE}*"
    rm -rf ${LOCAL_DATA}"/.*.crc"
    rm -rf ${LOCAL_LOG}"/${CJV_OLD_SDATE}*"
}


mkdir -p ${LOCAL_DATA}
mkdir -p ${LOCAL_LOG}

log_file=${LOCAL_LOG}"/${TIME_TAG}.log"
get_docs | tee -a ${log_file}

clear_old_data
