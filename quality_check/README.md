
[nonlocal质量例行评估与质量实验-confluence](https://particlemedia.atlassian.net/wiki/spaces/~412903070/pages/2369062305)

add crontab job:

``` shell
00 2 * * * cd /home/services/tangyuyang/tasks/quality_check/bin && bash start_run_crawl.sh
05 2 * * * cd /home/services/tangyuyang/tasks/quality_check/bin && bash start_run_analysis.sh
```
