[hadoop@10-10-6-255 ~]$ crontab -l
# tiangx
## batch push
0 5 * * * cd /home/hadoop/ftp_push && /bin/sh ./token_android_without_oppo.sh >> ./outlog_androidwithoutoppo.log 2>&1
## big push
31 5 * * * cd /home/hadoop/ftp_push && /bin/sh ./ftp_push.sh >> ./outlog_ftp.log 2>&1

#OMEGA
## 1:增量用户性别预测(每月16日中午调度)
#0 12 16 * * cd /home/hadoop/users/maoxy/gender && /bin/sh ./increment_register_gender.sh > ./gender.log 2>&1
## 2:增量用户年龄预测(每月17日中午调度)
#0 12 17 * * cd /home/hadoop/users/maoxy/age && /bin/sh ./increment_register_age.sh > ./age.log 2>&1
## 3:将每日新增好视频推送至REDIS
0 7 * * * cd /home/hadoop/users/maoxy/goodvideo && /bin/sh ./goodvideo.sh > ./goodvideo.log 2>&1
## 4:将达人uid及产生的TOP2视频同步至REDIS(包括两种版本)
32 4 * * * cd /home/hadoop/users/maoxy/talent_page && /bin/sh ./talent_page_video.sh > ./running_log.log 2>&1
35 4 * * * cd /home/hadoop/users/maoxy/talent_page && /bin/sh ./talent_page_video_e2.sh > ./running_log_e2.log 2>&1
## 5:客户端崩溃率监测
12 9-23 * * * cd /home/hadoop/users/maoxy/send_alert && /usr/local/bin/python ./androidBreakUp_readcookie.py >> android.log
13 9-23 * * * cd /home/hadoop/users/maoxy/send_alert && /usr/local/bin/python ./iosBreakUp_readcookie.py >> ios.log
## 6:爬取APP信息更新(每月增量更新)
00 08 02 * * cd /home/hadoop/users/maoxy/update_app_info && /bin/sh ./app_info_update.sh > ./running.log 2>&1
00 08 * * * cd /home/hadoop/users/maoxy/videolabel && /bin/sh ./videoLabel.sh > ./running.log 2>&1
## 7:将曝光量TOP1000大视频写入REDIS用于引导评论
01 07 * * * cd /home/hadoop/users/maoxy/comment_backstage && /bin/sh ./comment_backstage.sh > ./running.log 2>&1
## 8:语音搜索报警-错误率(暂停)
#00,15,30,45 6-23 * * * cd /home/hadoop/users/maoxy/voice_search_alert && /usr/local/bin/python ./voice_search_monitor.py >> ./running.log 2>&1
## 9:语音搜索报警-服务量(暂停)
#05,20,35,50 * * * * cd /home/hadoop/users/maoxy/voice_search_alert && /usr/local/bin/python ./voice_service_monitor.py >> ./running.log 2>&1
## 10:搜索位置曝光点击量 写入Redis
02 06 * * * cd /home/hadoop/users/maoxy/search_position_dvvv && /bin/sh ./search_position_dvvv.sh > ./running.log 2>&1



#jiangcx

## 视频画像缺失vid写入redis中,每日6点执行
#00 06 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /bin/sh ./lost_and_redis_routine.sh

## 视频画像统计 每日早8点执行
00 08 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /bin/sh ./routine.sh

## 发送视频画像统计结果邮件 每日早9点30分执行
30 09 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /data/anaconda2/envs/jcxpython3/bin/python my_mail.py > /home/jiangcx/workspace/hive/permanent_notification_bar/log/routine/mail.log 2>&1

## 视频画像关键字段统计 每日早8点10分执行, cstatus=2
10 08 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /bin/sh ./routine2.sh

## 发送视频画像关键字段统计结果邮件 每日早9点40分执行
31 09 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /data/anaconda2/envs/jcxpython3/bin/python my_mailv2.py > /home/jiangcx/workspace/hive/permanent_notification_bar/log/routine/mailv2.log 2>&1

## 视频画像与集群数据对比，统计每日缺失的vid 每日早9点20分执行
#20 08 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /bin/sh ./lost_routine.sh

## 发送缺失的vid结果邮件 每日早9点32分执行
#32 09 * * * cd /home/jiangcx/workspace/hive/permanent_notification_bar && /data/anaconda2/envs/jcxpython3/bin/python lost_mail.py > /home/jiangcx/workspace/hive/permanent_notification_bar/log/routine/lost_mail.log 2>&1

## 每日早6点30分 计算ema值
40 04 * * * cd /home/jiangcx/workspace/hive/cal_ema && /bin/sh ./cal_ema.sh >./running.log 2>&1

## 每日9点30分 执行query 非舞蹈类的pv统计, 基于ema的表
00 07 * * *  cd /home/jiangcx/workspace/hive/permanent_notification_bar && /bin/sh ./query_routine.sh >./running_query.log 2>&1

## 历史所有content_mp3刷库
30 * * * * cd /data/jiangcx/workspace/hive/his_content_mp3_update_all && /bin/sh ./crontab2.sh >./running.log 2>&1

## 统计不完整的画像字段 的vid
#30 03 * * * cd /home/jiangcx/workspace/hive/his_status_check && /bin/sh ./auto_everyday.sh >./running.log 2>&1

#sunjian
## 视频画像 syn kafka 读取最新状态  每日早8点30执行
30 08 * * * cd /home/hadoop/users/sunjian/workspace/pyspark/syn && /bin/sh ./process.sh 1>> ./stdout_syn_cstatus 2>&1

55 * * * * cd /home/hadoop/users/sunjian/workspace/pyspark/difftitle && /bin/sh ./process_title.sh 1>> ./process_title.stdout 2>&1
#sunjian
## 视频画像 白名单计算历史数据需要更新的数据 每日6点30执行
08 * * * * cd /home/hadoop/users/sunjian/workspace/pyspark/whiteuid && /bin/sh ./process_whiteuid.sh 1>> ./stdout_white_uid 2>&1
# 计算content_mp3
57 * * * * cd /home/hadoop/users/sunjian/workspace/pyspark/content_mp3_teacher && /bin/sh ./process.sh 1>> ./stdout_content_mp3 2>&1

# query 相关性数据每日早8点产出
01 08 * * * cd /home/jiangcx/workspace/hive/history_process  && /bin/sh ./query_relevant_routine.sh >./running.log 2>&1


##wangxp
#每天向redis中写入排名前100的music_id,每天凌晨4点执行
00 04 * * * cd /home/hadoop/users/wangxp/music_id_rank && /bin/sh ./music_id_rank.sh > ./running.log 2>&1