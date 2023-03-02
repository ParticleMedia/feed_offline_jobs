#!/bin/bash

buckets=("foryou_rank_exp_2022q3-v4 foryou_rank_exp_2022q3-control")
base_url="http://172.31.23.110:8017/NewsRecommender/BlendRecommendedNewsHandler?infinite=true&mode=ie&os=android&msource=organic&distributionChannel=yidian_debug&enableShortVideo=true&enableSocial=true&enableBriefing=true&briefingExp=true&bversion=read4u&fetchnew=true&refresh=1&push_refresh=0&version=020032&platform=1&net=wifi&appid=newsbreak&distribution=yidian_debug&internal=true&epoch=3&start=0&end=10&debug=true&pressure=true&skipFlush=true&skipLog=true"


uid_file="../data/uid_zip.txt"
MAX_TEST_NUM=1000

output="../data/monica/"
mkdir -p $output

cur_num=0
while read line
do
    cur_num=`expr $cur_num + 1`
    if [ $cur_num -gt $MAX_TEST_NUM ]; then
        cur_num=`expr $cur_num - 1`
        break
    fi

    echo "uid="$line
    arr=(${line})
    uid=${arr[0]}
    zip=${arr[1]}

    for bucket in $buckets
    do
      url="${base_url}&exps=${bucket}&zip=${zip}&userid=${uid}"
      echo $url
      #
      res_file="${output}${uid}.out.${bucket}"
      curl $url > $res_file
    done
    #break
done < $uid_file

echo ""
echo "finished. test_num=$cur_num  max_test_num=$MAX_TEST_NUM"
