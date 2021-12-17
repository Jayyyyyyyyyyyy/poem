#!/bin/sh

#source /ect/profile
#source ~/.bashrc
#export PATH=${PATH}
export JAVA_HOME=/usr/java/latest

datebuf=$1
########################################
hive=$HIVE_HOME/bin/hive
########################################
datebuf_5daysago=`date -d "- 5 day" "+%Y%m%d"`
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
echo "`date` [INFO] ----------- 1、 statistic of videoprofile begin ---------"
########################################

/home/hadoop/hadoop-2.6.0/bin/hadoop fs -test -e "/user/jiangcx/his_diff_vids_2/total_data_"${datebuf}"/_SUCCESS"
if [ $? -eq 0 ]; then
    echo ${datebuf}": total data have DONE"
    exit
else
  ip="10.42.4.78:8020"
  state=`/home/hadoop/hadoop-2.6.0/bin/hadoop  fs -ls hdfs://$ip/  2>&1`
  if [[ "${state}" =~ "standby" ]]; then
  ip="10.42.178.9:8020"
  fi
  echo ${ip}": is work"
  #10.42.178.9:8020
  /home/hadoop/hadoop-2.6.0/bin/hadoop fs -test -e "hdfs://"${ip}"/user/jiaxj/Apps/DocsInfo/"${datebuf}"/_SUCCESS"
  if [ $? -eq 0 ]; then
  echo 'execute spark'
  /home/hadoop/hadoop-2.6.0/bin/hadoop fs -rmr "/user/jiangcx/his_diff_vids_2/total_data_"${datebuf}
  {
  /home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /data/jiangcx/workspace/hive/his_content_mp3_update_all/get_field_es2.py $datebuf $ip
  } >> /data/jiangcx/workspace/hive/his_content_mp3_update_all/log/routine_${datebuf}.log 2>&1


  /home/hadoop/hadoop-2.6.0/bin/hadoop fs -rmr "/user/jiangcx/his_diff_vids_2/total_data_"${datebuf_5daysago}
  else
    echo ${datebuf}": jxj data is not exist, please wait again"
    exit
  fi
fi
################################################################################
echo "`date` [INFO] ----------- 1、 statistic of videoprofile  end ---------"
################################################################################