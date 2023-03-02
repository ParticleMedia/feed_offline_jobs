## steps

1.calculate history doc

    1.1 mod script: cjv_date, dim_doc_date, filter
    1.2 execute: `nohup python3 timeliness_longterm_engage_history.py > nohup.out.engage > 2>&1 &`

2.reflush history

    2.1 ssh host & cd path:`services@ip-172-31-22-245:~/liangshufei/tools/evergreen-proxy/bin`
    2.2 download history to local
    2.3 execute: `nohup ./flush_history --input=/data/tangyuyang/tasks/reflush_history/longterm_impr/2022-07-01_2022-08-01 --collection=timeliness_longterm --limit=2000 >nohop.out.impr 2>&1 &`
    2.4 source code: `https://github.com/ParticleMedia/evergreen-proxy/blob/main/tools/flush_history.go`
