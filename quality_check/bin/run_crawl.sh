#!/bin/bash
export LC_ALL=C

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

function prepare_data() {
    local output_path=${RUNNING_PATH}
    bash -x ${BIN_PATH}/run_init.sh 0 ${output_path} > ${RUNNING_LOG_PATH}/prepare.log 2>&1
    check_status "$?" "fatal: init prepare_data failed"
}

function write_data() {
    ${PYTHON3_BIN} ${BIN_PATH}/write_evaluation_docid.py ${CRAWL_DATA_FILE} ${WRITE_DATA_FILE} ${WRITE_LOG_FILE}
    check_status "$?" "fatal: write_data failed"


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
                ${PYTHON3_BIN} ${BIN_PATH}/write_evaluation_docid.py ${exp_data_file} ${WRITE_DATA_FILE} ${log_file}
            done
        else
            echo "exp parameter error"
        fi
    else
        echo "don't run write exp data"
    fi
}

source  ../conf/conf.sh

if [ -d ${RUNNING_PATH} ]
then
    mv ${RUNNING_PATH} ${RUNNING_PATH}".old"
fi
mkdir -p ${RUNNING_PATH}
mkdir -p ${RUNNING_LOG_PATH}

prepare_data
check_status "$?" "fatal: prepare_data failed"

write_data
check_status "$?" "fatal: write_data failed"