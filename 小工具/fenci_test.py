
with open('/Users/jiangcx/Documents/nohup.out', 'r', encoding='utf-8') as f, open('/Users/jiangcx/Documents/newlog', 'w', encoding='utf-8') as f2:
    for ind, line in enumerate(f):
        if 'createtime' in line:
            f2.write(line)
