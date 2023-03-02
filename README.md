# feed_offline_jobs
* 本代码库保存feed离线任务
* 与[feed_offline_tasks](https://github.com/ParticleMedia/feed_offline_tasks)代码库的区别：
  * 这里的任务不是统一的执行方式

## 例行任务
* 根目录下新建任务目录,例如`quality_check`
* 执行任务的启动方式放到`crontab.txt`中

## 非例行任务
* 统一存放目录： `non_schedule_job/`