
NOW_DATE=`date +"%Y-%m-%d"`
cd /home/services/tangyuyang/tasks/quality_check/bin
log_path="../log/${NOW_DATE}"
mkdir -p ${log_path}

bash -x run_crawl.sh > ${log_path}/cron.crawl.log.out 2> ${log_path}/cron.crawl.log.err
