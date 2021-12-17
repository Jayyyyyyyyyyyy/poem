
ps -ef |  grep 5012 |  awk -F' ' '{print $2}' | xargs kill -9

