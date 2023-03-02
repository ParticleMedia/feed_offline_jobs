#!/bin/bash

ALARM_EMAILS="yuyang.tang@newsbreak.com"
if [ ! $NOW_DATE ] ; then
    NOW_DATE=`date +"%Y-%m-%d"`
fi
YESTERDAY_DATE=`date -d "1 day ago" +%Y-%m-%d`

WORK_PATH="/home/services/tangyuyang/tasks/quality_check"
DATA_PATH="${WORK_PATH}/data"
BIN_PATH="${WORK_PATH}/bin"
LOG_PATH="${WORK_PATH}/log"
CONF_PATH="${WORK_PATH}/conf"
RUNNING_PATH="${DATA_PATH}/${NOW_DATE}"
RUNNING_LOG_PATH="${LOG_PATH}/${NOW_DATE}"

UID_FILE="${RUNNING_PATH}/uid.list"
CRAWL_DATA_FILE="${RUNNING_PATH}/crawl.data"
WRITE_DATA_FILE="${RUNNING_PATH}/write.data"
WRITE_LOG_FILE="${RUNNING_LOG_PATH}/write.log"

READ_DATA_FILE="${RUNNING_PATH}/read.data"
READ_LOG_FILE="${RUNNING_LOG_PATH}/read.log"
ANALYSIS_DATA_FILE="${RUNNING_PATH}/analyze.data"
ANALYSIS_LOG_FILE="${RUNNING_LOG_PATH}/analyze.log"
CHECK_READ_DATA_FILE="${RUNNING_PATH}/read.data.double_check"

# online address & config
DEBUG_HTML_PREFIX="http://monica.ha.nb.com:8017/NewsRecommender/BlendRecommendedNewsHandler?appid=newsbreak&bversion=read4u&campaign=newsbreak_ios_bigcity_v3_d16_005%2CMilpitas%2CCA&display_name=Milpitas%2C+CA&distribution=com.apple.appstore&distributionChannel=com.apple.appstore&enableBriefing=true&enableShortVideo=false&enableSocial=true&end=25&fetchnew=true&following_guide=true&infinite=true&mode=ie&msource=Facebook+Ads&net=wifi&os=ios&platform=0&refresh=1&start=0&version=020047&skipFlush=true&debug=true"
STATIC_HTML_PREFIX="http://doc-profile.ha.nb.com:9600/get?from=test&fields=static_feature.*"
MAX_UID_NUM=200

PYTHON2_BIN="/usr/bin/python2"
PYTHON3_BIN="/home/services/anaconda3/bin/python3"

################################################
# 开启实验抓取，OPEN_EXP_EVALUATION=1时，EXP_*参数才生效；仅支持一个实验一个bucket；label与bucket个数对应，多个时分别使用空格分隔
OPEN_EXP_EVALUATION=1
EXP_LABELS="clickbait_apply"
EXP_BUCKEXT="foryou_rank_exp_2022q3-v4"
