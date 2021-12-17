while [ 1 ]
do

pid=`ps -axu | grep 9026 | grep python | awk '{print $2}' | head -n 1`
if [ "$pid" = "" ];then
  sh start.sh
else
  echo "server is running well"
fi
  sleep 60
done
