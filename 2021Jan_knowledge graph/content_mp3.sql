SELECT content_mp3,
       cnt
FROM
  (SELECT p_content_mp3["tagname"] AS content_mp3,
          count(*) AS cnt
   FROM dw.video_profile_parse
   WHERE dt = "2021-02-03"
     AND f_cstage IN (6,
                      7,
                      8,
                      10)
     AND f_ctype IN (101,
                     102,
                     103,
                     105,
                     106,
                     107,
                     301,
                     121)
     AND f_cstatus=0
   GROUP BY p_content_mp3["tagname"])a
WHERE cnt > 10