#!/bin/bash
export LC_ALL=C
source  ../conf/conf.sh

function send_mail_msg() {
    local subject="$1"
    local msg="$2"
    local emails=${ALARM_EMAILS//,/ }
    for email in ${emails}; do
        echo "${msg}" | mail -s "${subject}" ${email}
    done
    return 0
}

function check_status() {
    if [ $# -ne 2 ]
    then
        send_mail_msg "qualityQueue" "wrong param for check_status"
        exit 1
    fi

    if [ $1 -ne 0 ]
    then
        send_mail_msg "qualityQueue" "$2"
        exit 1
    fi
    return 0
}


function do_read_analysis_data() {
    crawl_data_file=$1
    read_data_file=$2
    check_read_data_file=$3
    read_log_file=$4
    analysis_data_file=$5
    analysis_log_file=$6

    ${PYTHON3_BIN} ${BIN_PATH}/read_review_from_mongo.py ${crawl_data_file} ${read_data_file} ${read_log_file}

    if [ ! -f ${read_data_file} ]; then
        echo "read_data_file not exists, skip this execution [${read_data_file}]"
        return
    fi

    ${PYTHON3_BIN} ${BIN_PATH}/double_check_review.py ${read_log_file} ${check_read_data_file}

    ${PYTHON3_BIN} ${BIN_PATH}/analysis_review.py ${read_data_file} ${analysis_data_file} ${analysis_log_file}
}


function read_analysis_data_exp() {
    if [ ${OPEN_EXP_EVALUATION} -eq 1 ]; then
        echo "run write exp data"
        label_arr=(${EXP_LABELS})
        bucket_arr=(${EXP_BUCKEXT})

        l1=${#label_arr[*]}
        l2=${#bucket_arr[*]}
        if [ $l1 -eq $l2 ]; then
            for((i=0; i<$l2; ++i))
            do
                label=${label_arr[$i]}
                bucket=${bucket_arr[$i]}
                echo "round#$i label=${label} bucket=${bucket}"

                exp_data_file="${CRAWL_DATA_FILE}.exp.${label}"
                log_file="${WRITE_LOG_FILE}.exp.${label}"

                crawl_data_file="${CRAWL_DATA_FILE}.exp.${label}"
                read_data_file="${READ_DATA_FILE}.exp.${label}"
                check_read_data_file="${CHECK_READ_DATA_FILE}.exp.${label}"
                analysis_data_file="${ANALYSIS_DATA_FILE}.exp.${label}"

                do_read_analysis_data ${crawl_data_file} ${read_data_file} ${check_read_data_file} ${READ_LOG_FILE} ${analysis_data_file} ${ANALYSIS_LOG_FILE}
            done
        else
            echo "exp parameter error"
        fi
    else
        echo "don't run write exp data"
    fi
}

function read_analysis_data() {
    do_read_analysis_data ${CRAWL_DATA_FILE} ${READ_DATA_FILE} ${CHECK_READ_DATA_FILE} ${READ_LOG_FILE} ${ANALYSIS_DATA_FILE} ${ANALYSIS_LOG_FILE}
    check_status "$?" "fatal: analyze_review failed"
}

mkdir -p ${RUNNING_PATH}
mkdir -p ${RUNNING_LOG_PATH}

# run for last 7 day
for i in `seq 1 7`
do
    NOW_DATE=`date -d "${i} days ago" +%Y-%m-%d`
    source  ../conf/conf.sh
    echo "run analysis date=${NOW_DATE}"

    read_analysis_data

    read_analysis_data_exp

    cp ../data/${NOW_DATE}/read.data /mnt/nlp/xingguang/mac_desk/nlu_qua/datas/raw_datas/quality_check_unlabel/${NOW_DATE}
done

echo "job successful"