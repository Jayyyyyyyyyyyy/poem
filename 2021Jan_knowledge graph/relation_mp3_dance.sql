select p_content_mp3['tagname'] as p_content_mp3, p_content_dance['tagname'] as p_content_dance, count(*)
from dw.video_profile_parse
where dt = date_sub(current_date ,1)
    and p_content_mp3['tagname'] not in ('-', '', 'unknown')
    and p_content_dance['tagname'] not in ('-', '', 'unknown')
    and f_cstage in (6,7,8,10)    --已粗标 可分发
    and f_ctype in (101, 102, 103, 105, 106, 107, 301, 121)    --视频类型: 大、小视频等横向视频
    and f_cstatus=0               --正常
group by p_content_mp3['tagname'], p_content_dance['tagname']


