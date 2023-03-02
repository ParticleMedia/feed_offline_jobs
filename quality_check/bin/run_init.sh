#!/bin/bash
export LC_ALL=C

# online address & config
source ../conf/conf.sh

function check_status() {
    if [ $# -ne 2 ]
    then
        echo "wrong param for check_status" >&2
        exit 1
    fi

    if [ $1 -ne 0 ]
    then
        echo "$2" >&2
        exit 1
    fi
    return 0
}

function monica_foryou_extract() {
    if [ $# -ne 4  ]
    then
        echo "wrong param for monica_foryou_extract" >&2
        return 1
    fi

    local is_local=$1
    local out_file=$2
    local is_exp=$3
    local bucket=$4
    local mid_html="mid.html"
    local mid_out="mid.out"
    local mid_url="mid.url"

    rm ${out_file} >/dev/null 2>&1

    echo "extract_parameter: is_local=${is_local} out_file=${out_file} is_exp=${is_exp} bucket=${bucket}"

    while read line
    do
        # parse debug for uid
        # local uid=`echo ${line} | tr -d '\r'`
        arr=(${line})
        uid=${arr[0]}
        zip=${arr[1]}
        echo "url-parameter: ${uid} ${zip}"

        local debug_html_path="${DEBUG_HTML_PREFIX}&userid=${uid}&zip=${zip}"
        if [ ${is_exp} -eq 1 ]; then
            debug_html_path="${DEBUG_HTML_PREFIX}&userid=${uid}&zip=${zip}&exps=${bucket}"
        fi

        wget ${debug_html_path} -O ${mid_html} >/dev/null 2>&1
        if [ $? -ne 0 ]
        then
            echo "wrong: get html for ${uid}" >&2
            continue
        fi
        ${PYTHON2_BIN} tool.py "parse_foryou" "${is_local}" "${uid}" "${mid_html}" >${mid_out}
        if [ $? -ne 0  ]
        then
            echo "wrong: parse foryou for ${uid}" >&2
            continue
        fi

        # parse info for docid
        local doc_ids=`cat ${mid_out} | awk 'BEGIN{FS="\t"} {print $2}'`
        for doc_id in ${doc_ids}
        do
            local static_html_path="${STATIC_HTML_PREFIX}&docids=${doc_id}"
            wget "${static_html_path}" -O ${mid_html} >/dev/null 2>&1
            if [ $? -ne 0 ]
            then
                local fake_info="${doc_id}\tnullTitle\tnullPubTime"
                echo -e "${fake_info}"
            else
                local info=`${PYTHON2_BIN} tool.py "parse_url_info" "${NOW_DATE}" "${mid_html}"`
                echo "${info}"
            fi
        done > ${mid_url} 2>/dev/null

        # check valid
        mid_debug_num=`wc -l ${mid_out} | sed 's/^[ \t ]*//g' | awk '{print $1}'`
        mid_url_num=`wc -l ${mid_url} | sed 's/^[ \t ]*//g' | awk '{print $1}'`
        if [ ${mid_debug_num} -ne ${mid_debug_num} ]
        then
            continue # skip curr uid
        fi

        paste -d $'\t' ${mid_out} ${mid_url} >> ${out_file}
    done < ${UID_FILE}

    rm ${mid_html} >/dev/null 2>&1
    rm ${mid_out} >/dev/null 2>&1
    rm ${mid_url} >/dev/null 2>&1
    return 0
}

function uniq_for_assess() {
    if [ $# -ne 3 ]
    then
        echo "wrong param for uniq_for_assess" >&2
        return 1
    fi

    max_assess_num=$1
    in_data=$2
    out_data=$3
    ${PYTHON2_BIN} tool.py "uniq_for_assess" "${max_assess_num}" "${in_data}" > "${out_data}"
    if [ $? -ne 0  ]
    then
        echo "wrong: uniq_for_assess" >&2
        return 1
    fi
}


is_local=0
if [ $# -eq 1 ]
then
    is_local=$1
fi

if [ -d ${RUNNING_PATH} ]
then
    rm -rf ${RUNNING_PATH}
fi
mkdir -p ${RUNNING_PATH}

# random uid
${PYTHON2_BIN} uid_impala_extract.py "${YESTERDAY_DATE}" "${MAX_UID_NUM}" > ${UID_FILE}
check_status "$?" "fatal: random_uid_extract"

# parse json
monica_foryou_extract "${is_local}" "${CRAWL_DATA_FILE}" "0" "online"
check_status "$?" "fatal: monica_foryou_extract_online failed"

# evaluation exp with bucket parameter
if [ ${OPEN_EXP_EVALUATION} -eq 1 ]; then
    echo "run extract exp data"
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
            # parse json
            monica_foryou_extract "${is_local}" "${exp_data_file}" "1" "${bucket}"
        done
    else
        echo "exp parameter error"
    fi
else
    echo "don't run exp extract data"
fi



mkdir -p /mnt/nlp/xingguang/mac_desk/nlu_qua/datas/raw_datas/quality_check_unlabel/${NOW_DATE}/
cp ${CRAWL_DATA_FILE}* /mnt/nlp/xingguang/mac_desk/nlu_qua/datas/raw_datas/quality_check_unlabel/${NOW_DATE}/
