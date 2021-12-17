
import pandas as pd
import json
import shutil
import sys
import os
## python parse_log.py 2020-06-09 user_imgs_2020-06-09
yesterday = sys.argv[1]
raw_data_path = sys.argv[2]
tmp = []

def my_write(path, content):
    img_collections = './img_{}'.format(yesterday)
    if not os.path.exists(img_collections):
        os.mkdir(img_collections)
    path = os.path.join(img_collections, path)
    with open(path,'w') as w:
        w.write(content)


with open(raw_data_path, 'r') as reader:
    for line in reader:
        try:
            if '}{' in line:
                line = line.replace('}{','}\t{')
                for x in line.split('\t'):
                    x = json.loads(x.strip())
                    score = x['score']
                    frame = x['teacher_frame_time']
                    uid = x['uid']
                    time = x['upload_time']
                    details = x['details']
                    file_name = '{}_{}'.format(uid, time)
                    try:
                        my_write(file_name, x['img'])
                    except:
                        pass
                    tmp.append([uid, frame, time, score, details])
            else:
                line = json.loads(line.strip())
                score = line['score']
                frame = line['teacher_frame_time']
                uid = line['uid']
                time = line['upload_time']
                details = line['details']
                file_name = '{}_{}'.format(uid, time)
                try:
                    my_write(file_name, line['img'])
                except:
                    pass
                tmp.append([uid, frame, time, score, details])
        except:
            print(line)

df = pd.DataFrame(tmp, index=None, columns=['uid', 'frame', 'time', 'score', 'details'])
df = df.loc[df.groupby(['uid','frame'])['score'].idxmax()]
df_file = './stats/stat_{}.csv'.format(yesterday)
df.to_csv(df_file, index=None)

df_score = df.sort_values('frame')
df_score = df_score.groupby(['frame','score']).agg({'frame': ['size']})
df_score.columns = ['count']
df_score = df_score.reset_index()
df_score = df_score.loc[df_score.score == 0]
df_total = df.groupby(['frame']).agg({'frame': ['count']})
df_total.columns = ['count_total']
df_total = df_total.reset_index()
new = pd.merge(df_score, df_total,on='frame')
new = new[['frame', 'count', 'count_total']]
new['ratio']=new['count']/new['count_total']
new = new.sort_values('ratio')
out_file = './stats/stat_zero_rate_{}.csv'.format(yesterday)
new.to_csv(out_file, index=None)


# easier_df = new[ (new['ratio']>=0.24)  &  (new['ratio']<0.43)]
# easy_df = new[ (new['ratio']>=0.43) &  (new['ratio']<0.62 ) ]
# hard_df = new[ (new['ratio']>=0.62) &  (new['ratio']<0.81 ) ]
# harder_df = new[ (new['ratio']>=0.81) &  (new['ratio']<100 ) ]

# def cp_files(df, file):
#     frames = df['frame']
#     for frame in frames:
#         try:
#             #infile = '/Users/tangdou1/PycharmProjects/poem/2020Apr_dance_machine/dance_score_server/data/teacher/frame{}.jpg'.format(frame)
#             infile = '/Users/tangdou1/PycharmProjects/poem/2020Apr_dance_machine/dance_score_server/data/output_teacher/frame{}_rendered.png'.format(frame)
#             outfile = '/Users/tangdou1/PycharmProjects/poem/2020Apr_dance_machine/result_analysis/imgs/{}/frame{}.jpg'.format(file, frame)
#             shutil.copy(infile, outfile)
#         except:
#             print(frame)

# cp_files(easier_df,'easier')
# cp_files(easy_df,'easy')
# cp_files(hard_df,'hard')
# cp_files(harder_df,'harder')

