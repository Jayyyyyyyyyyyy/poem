#!/bin/sh

source /etc/profile
source ~/.bashrc
export PATH=${PATH}
export JAVA_HOME=/usr/java/latest
datebuf=$1
########################################
hadoop=$HADOOP_HOME/bin/hadoop
hive=$HIVE_HOME/bin/hive
########################################

datebuf=$1
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
mydatebuf=${datebuf:2:9}

today=$(date +%Y%m%d)
today2=$(date +%Y-%m-%d)
yesterday=$(date +%Y%m%d --date '1 days ago')
yesterday2=$(date +%Y-%m-%d --date '1 days ago')
curtime=$(date +'%Y-%m-%d %H:%M:%S')
yesterday2=$(date +%Y-%m-%d --date '1 days ago')

echo "`date` [INFO] ----------- rundate:" $datebuf


########################################
echo "`date` [INFO] ----------- job begin ---------"
########################################


#########################################
echo "`date` [INFO] ----------- 1、 get data from hdfs begin ---------"
#########################################

hdfs_data="/ai/dance_machine/"${mydatebuf}
local_data="user_imgs_"${datebuf}
$hadoop fs -test -e $hdfs_data
if [ $? -eq 0 ]; then
   echo $curtime"  hdfs_file "$hdfs_data" is exit, process begin"
   $hadoop fs -cat ${hdfs_data}"/*/*" > ${local_data}
else
    echo $curtime"  hdfs_file "$hdfs_data" is not exist, please wait again"
    exit
fi
################################################################################
echo "`date` [INFO] ----------- 1、 get data from hdfs end ---------"
################################################################################


#########################################
echo "`date` [INFO] ----------- 2、 parse begin ---------"
#########################################
python parse_log.py $datebuf $local_data
################################################################################
echo "`date` [INFO] ----------- 2、 parse end ---------"
################################################################################


#########################################
echo "`date` [INFO] ----------- 3、 delete begin ---------"
#########################################
six_day_ago=`date -d "- 6 day" "+%Y-%m-%d"`
rm -rf "user_imgs_"${six_day_ago} "img_"${six_day_ago}
################################################################################
echo "`date` [INFO] ----------- 3、 delete end ---------"
################################################################################


#########################################
echo "`date` [INFO] ----------- 4、 send mail begin ---------"
#########################################
python auto_mail.py
################################################################################
echo "`date` [INFO] ----------- 4、 send mail end ---------"
################################################################################

########################################
echo "`date` [INFO] ----------- job end ---------"
########################################



