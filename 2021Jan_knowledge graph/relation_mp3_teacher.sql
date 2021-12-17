select p_content_mp3['tagname'] as p_content_mp3, p_content_teacher['tagname'] as p_content_teacher, count(*)
from dw.video_profile_parse
where dt = date_sub(current_date ,1)
    and p_content_mp3['tagname'] not in ('-', '', 'unknown')
    and p_content_teacher['tagname'] not in ('-', '', 'unknown')
    and f_cstage in (6,7,8,10)    --已粗标 可分发
    and f_ctype in (101, 102, 103, 105, 106, 107, 301, 121)    --视频类型: 大、小视频等横向视频
    and f_cstatus=0               --正常
group by p_content_mp3['tagname'], p_content_teacher['tagname']





select vid, title, uid, uname, content_raw, content_mp3, content_teacher, createtime from
(

select v_vid as vid, v_title as title, p_uid as uid, p_uname as uname,
p_content_raw as content_raw, bb.p_content_mp3 as content_mp3, bb.p_content_teacher as content_teacher, v_createtime as createtime
from
(select p_content_mp3, p_content_teacher, cnt from
(select p_content_mp3['tagname'] as p_content_mp3, p_content_teacher['tagname'] as p_content_teacher, count(*) as cnt
from dw.video_profile_parse
where dt = date_sub(current_date ,1)
    and p_content_mp3['tagname'] not in ('-', '', 'unknown')
    and p_content_teacher['tagname'] not in ('-', '', 'unknown')
    and f_cstage in (6,7,8,10)    --已粗标 可分发
    and f_ctype in (101, 102, 103, 105, 106, 107, 301, 121)    --视频类型: 大、小视频等横向视频
    and f_cstatus=0               --正常
group by p_content_mp3['tagname'], p_content_teacher['tagname'])a
where cnt  >= 5)aa
left join
(select v_vid, v_title, p_uid, p_uname, p_content_raw['tagname'] as p_content_raw, p_content_mp3['tagname'] as p_content_mp3, p_content_teacher['tagname'] as p_content_teacher, v_createtime
from dw.video_profile_parse
where dt = date_sub(current_date ,1)
    and v_vid is not null
    and p_content_mp3['tagname'] not in ('-', '', 'unknown')
    and p_content_teacher['tagname'] not in ('-', '', 'unknown')
    and f_cstage in (6,7,8,10)    --已粗标 可分发
    and f_ctype in (101, 102, 103, 105, 106, 107, 301, 121)    --视频类型: 大、小视频等横向视频
    and f_cstatus=0)bb  --正常
on (aa.p_content_mp3=bb.p_content_mp3 and aa.p_content_teacher=bb.p_content_teacher)  --)cc
where aa.p_content_mp3 is not null and aa.p_content_teacher is not null
-- group by vid, title, uid, uname, content_raw, content_mp3, content_teacher, createtime