# !/bin/bash
set -x
set -e
set -u

##### runtime conf
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
PYTHON_BIN="/home/services/miniconda3/bin/python3"
TIME_TAG=`date +%Y%m%d%H`

##### task conf
TASK_NAME="local_geo_top_docs_channel"

HDFS_PATH="s3a://pm-hdfs2/user/tangyuyang/${TASK_NAME}"
LOCAL_DATA="/data/tangyuyang/${TASK_NAME}/data"
LOCAL_LOG="/data/tangyuyang/${TASK_NAME}/log"

# redis
REDIS_PREFIX="local_geo_top"
REDIS_TTL_SEC=86400

# sql
CJV_DATE_DIFF=3
DOC_DATE_DIFF=3
CJV_SDATE=`date +%Y-%m-%d -d "-${CJV_DATE_DIFF} days"`
DOC_SDATE=`date +%Y-%m-%d -d "-${DOC_DATE_DIFF} days"`
MIN_CHECK=100
MAX_DOC_IN_GEO=200   # 单个key保存的结果数

CLEAR_DAY=5
MIN_RESULT_NUM=100


function get_docs() {
    local hdfs_cjv_path=${HDFS_PATH}"/data_${TIME_TAG}"
    local merged_file=${LOCAL_DATA}/${TIME_TAG}_data
    local kv_file=${LOCAL_DATA}/${TIME_TAG}_kv
    local hive_sql="
WITH geo_doc AS (
  SELECT
    channel,
    concat(exid.type, '@', exid.pid) as type_pid,
    concat(channel, '@', concat(exid.type, '@', exid.pid)) as cate_pid,
    doc.doc_id,
    doc.publish_time
  FROM dim.document_parquet doc
  LATERAL VIEW explode(geotag) idtable as exid
  LATERAL VIEW explode(channels) tmpTable AS channel
  WHERE doc.pdate >= '${DOC_SDATE}'
    AND channel IS NOT NULL
    AND exid.pid IS NOT NULL
    AND exid.type != 'state'
)

, cate_geo_cjv AS (
  SELECT
      cate_pid,

      round(1.0000 * sum(cjv.clicked) / sum(cjv.checked), 4) as ctr,
      sum(cjv.checked) as check,
      sum(cjv.clicked) as click
    FROM warehouse.online_cjv_hourly cjv
    LEFT JOIN geo_doc ON geo_doc.doc_id = cjv.doc_id
    WHERE
      cjv.joined = 1
      AND cjv.pdate >= '${CJV_SDATE}'
      AND cjv.channel_name in ('foryou', 'local')
      AND (cjv.nr_condition rlike 'local')
      AND cjv.checked = 1
      AND cate_pid IS NOT NULL
    GROUP BY cate_pid
)

insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by '\t'

SELECT
  *
FROM (
    SELECT
      cate_pid,
      cjv.doc_id,

      round(1.0000 * sum(cjv.clicked) / sum(cjv.checked), 4) as ctr,
      sum(cjv.checked) as check,
      sum(cjv.clicked) as click
    FROM warehouse.online_cjv_hourly cjv
    LEFT JOIN geo_doc ON geo_doc.doc_id = cjv.doc_id
    WHERE
      cjv.joined = 1
      AND cjv.pdate >= '${CJV_SDATE}'
      AND cjv.channel_name in ('foryou', 'local')
      AND (cjv.nr_condition rlike 'local')
      AND cjv.checked = 1
      AND cate_pid IS NOT NULL
    GROUP BY cate_pid, cjv.doc_id
) t
LEFT JOIN cate_geo_cjv ON cate_geo_cjv.cate_pid = t.cate_pid
WHERE t.check >= ${MIN_CHECK}
  AND t.ctr >= cate_geo_cjv.ctr
ORDER BY t.cate_pid, t.ctr DESC
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
        --output ${kv_file} \
        --max_size_in_key ${MAX_DOC_IN_GEO}

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
