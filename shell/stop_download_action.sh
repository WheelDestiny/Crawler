#!/bin/sh
echo "stop download news action"
user=`/usr/bin/whoami`
PID="`ps -ef|grep python|grep ${user}|grep download_news_action|grep -v grep|awk '{print $2}'`"
kill ${PID}
echo "stop finish..."
