#!/bin/bash

buckets=("local_strategy_22q3_exp-v10 local_strategy_22q3_exp-control")
base_url="http://172.31.23.110:8025/serving/channel?type=local&client=monica&from_id=k1174&start=0&end=500&explain=true&pressure=true"

uid_file="../data/uid_zip.txt"
MAX_TEST_NUM=1000

output="../data/local/"
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
      url="${base_url}&bucket=${bucket}&zip=${zip}&userid=${uid}"
      echo $url
      #
      res_file="${output}${uid}.out.${bucket}"
      curl $url > $res_file
    done
    #break
done < $uid_file

echo ""
echo "finished. test_num=$cur_num  max_test_num=$MAX_TEST_NUM"
