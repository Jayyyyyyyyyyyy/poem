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
##########################################
#echo "`date` [INFO] ----------- 1、 content_mp3 ---------"
##########################################
#{
#hive -f content_mp3.sql | sed 's/[\t]/,/g'  > ./tmp_content_mp3.csv
#} >> /data/jiangcx/knowledge_graph/logs/content_mp3_sql${datebuf}.log 2>&1
#python csv_generator.py tmp_content_mp3.csv content_mp3 ${datebuf}
#################################################################################
#echo "`date` [INFO] ----------- 1、 content_mp3  end ---------"
#################################################################################
#
#
##########################################
#echo "`date` [INFO] ----------- 2、 content_teacher ---------"
##########################################
#{
#hive -f content_teacher.sql | sed 's/[\t]/,/g'  > ./tmp_content_teacher.csv
#} >> /data/jiangcx/knowledge_graph/logs/content_teacher_sql${datebuf}.log 2>&1
#python csv_generator_content_teacher.py tmp_content_teacher.csv content_teacher ${datebuf}
#################################################################################
#echo "`date` [INFO] ----------- 2、 content_teacher  end ---------"
#################################################################################

##########################################
#echo "`date` [INFO] ----------- 3、 relation_mp3_teacher ---------"
##########################################
#{
#hive -f relation_mp3_teacher.sql | sed 's/[\t]/,/g'  > ./tmp_relation_mp3_teacher.csv
#} >> /data/jiangcx/knowledge_graph/logs/relation_mp3_teacher_sql${datebuf}.log 2>&1
#python csv_generator_relation_mp3_teacher.py tmp_relation_mp3_teacher.csv ${datebuf}
#################################################################################
#echo "`date` [INFO] ----------- 3、 relation_mp3_teacher  end ---------"
#################################################################################


##########################################
#echo "`date` [INFO] ----------- 4、 content_dance ---------"
##########################################
#{
#hive -f content_dance.sql | sed 's/[\t]/,/g'  > ./tmp_content_dance.csv
#} >> /data/jiangcx/knowledge_graph/logs/content_dance_sql${datebuf}.log 2>&1
#python csv_generator_content_teacher.py tmp_content_dance.csv content_dance ${datebuf}
#################################################################################
#echo "`date` [INFO] ----------- 4、 content_dance  end ---------"
#################################################################################


#########################################
echo "`date` [INFO] ----------- 5、 relation_mp3_dance ---------"
#########################################
{
hive -f relation_mp3_dance.sql | sed 's/[\t]/,/g'  > ./tmp_relation_mp3_dance.csv
} >> /data/jiangcx/knowledge_graph/logs/relation_mp3_dance_sql${datebuf}.log 2>&1
python csv_generator_relation_two_nodes.py tmp_relation_mp3_teacher.csv content_mp3 content_dance ${datebuf}
################################################################################
echo "`date` [INFO] ----------- 5、 relation_mp3_dance  end ---------"
################################################################################
