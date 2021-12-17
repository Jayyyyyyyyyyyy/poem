#!/bin/bash

source /etc/profile
export PATH=${PATH}

########################################
hadoop=$HADOOP_HOME/bin/hadoop
hive=$HIVE_HOME/bin/hive
spark=$SPARK_HOME/bin/spark-submit
########################################

date=`date -d "1 day ago" +"%Y%m%d"`
year=`date -d "$date" +"%Y"`
month=`date -d "$date" +"%m"`
day=`date -d "$date" +"%d"`

datebuf=$1

if [ -z "$1" ] ;then
year=$year
month=$month
day=$day
else
if [ -n "$1" ] && [ ${#datebuf} -eq 10 ]; then
year=${datebuf:0:4}
month=${datebuf:5:2}
day=${datebuf:8:2}
else
echo "`date` [ERROR] ----------- parameter error! please check it once again! dateformat eg:2013-09-01"
exit 0
fi
fi


datebuf=$year-$month-$day

##########################################################################################




##########################################################################################
###
checkexsits=0;
times=0;

#每次5分钟*120次 等待10小时


if ! [ $checkexsits -eq 1 ]; then
    echo "Success file check compelet: _SUCCESS not exits!"
fi


hadoop distcp -m 5 -i -overwrite hdfs://10.42.31.63:8020/olap/db/user/ /olap/db/userr/

$spark --queue root.spark --master yarn --conf spark.yarn.dist.archives=hdfs://Ucluster/user/caoyc/mypython.tar.gz#mypython --conf spark.pyspark.python=./mypython/pyenv/bin/python --conf spark.pyspark.driver.python=/home/hadoop/yc/anaconda2/bin/python --deploy-mode client --py-files ./dateutil.zip,./six.py --num-executors 30 --executor-cores 5 --executor-memory 12g  ./social_recall.py $datebuf


#00 10 * * * source /home/zhaoyf/.bashrc && cd /home/zhaoyf/yinfeng/crontab_social_recall && sh coordinator_social_recall.sh > ./coordinator.log 2>&1


