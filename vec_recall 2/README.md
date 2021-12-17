## 1. start service

如果需要更新数据源：

- 需要准备数据文件，并重命名为`img_feature_all_YY-MM-DD`放到目录：`similarity/data/update_data/`下
- 并且清空`similarity/data/`下面的`.pkl`和`.ann`文件

执行开始脚本

```shell
./start.sh
```

## 2. stop service

```shell
./stop.sh
```

## 3. 接口说明

参考`/test/example.py`
