SELECT p_content_teacher['tagname'] AS p_content_teacher,
       p_content_teacher['tagid'] AS p_content_teacher,
       count(*)
FROM dw.video_profile_parse
WHERE dt = date_sub(CURRENT_DATE,1)
  AND p_content_teacher['tagname'] not in ('' , '-', 'unknown')
  AND f_cstage IN (6,
                   7,
                   8,
                   10)--已粗标 可分发
  AND f_ctype IN (101,
                  102,
                  103,
                  105,
                  106,
                  107,
                  301,
                  121)--视频类型: 大、小视频等横向视频
  AND f_cstatus=0 --正常
GROUP BY p_content_teacher['tagname'], p_content_teacher['tagid'] ;