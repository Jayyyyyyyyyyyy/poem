#!/bin/sh

#source /ect/profile
#source ~/.bashrc
#export PATH=${PATH}

datebuf=$1
########################################
hadoop=$HADOOP_HOME/bin/hadoop
hive=$HIVE_HOME/bin/hive
########################################

my_date=`date -d "- 1 day" "+%Y%m%d"` #指定某一天
year=`date -d "$my_date" +"%Y"`
month=`date -d "$my_date" +"%m"`
day=`date -d "$my_date" +"%d"`


if [ -z "$1" ] ;then
year=$year
month=$month
day=$day
else
if [ -n "$1" ] && [ ${#datebuf} -eq 10 ]; then #${#datebuf}获取变量值的长度
year=${datebuf:0:4}
month=${datebuf:5:2}
day=${datebuf:8:2}
else
echo "`date` [ERROR] ----------- parameter error! please check it once again! dateformat eg:2013-09-01"
exit 0
fi
fi

date_con=$year$month$day
datebuf=$year-$month-$day

echo "`date` [INFO] ----------- rundate:" $datebuf


########################################
echo "`date` [INFO] ----------- job begin ---------"
########################################
#
#
#########################################
echo "`date` [INFO] ----------- 1、 statistic of videoprofile begin ---------"
#########################################
{
/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 20 --executor-cores 4 --executor-memory 4g /home/hadoop/users/jcx/hive/permanent_notification_bar/lost_and_redis.py $datebuf
} >> /home/hadoop/users/jcx/hive/permanent_notification_bar/log/routine/lost_and_redis_routine_${datebuf}.log 2>&1
################################################################################
echo "`date` [INFO] ----------- 1、 statistic of videoprofile  end ---------"
################################################################################

