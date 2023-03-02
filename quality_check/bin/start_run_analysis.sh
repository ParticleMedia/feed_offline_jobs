
NOW_DATE=`date +"%Y-%m-%d"`
cd /home/services/tangyuyang/tasks/quality_check/bin
log_path="../log/${NOW_DATE}"
mkdir -p ${log_path}

bash -x run_analysis.sh ${log_path}/cron.analysis.log.out 2> ${log_path}/cron.analysis.log.err