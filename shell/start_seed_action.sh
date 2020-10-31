#!/bin/sh
echo "start news find action"
nohup /usr/local/bin/python /home/wheeldestiny26/hainiu_crawler/Download_page/news_find_action.py >> /home/wheeldestiny26/python/hainiu_crawler/shell/news_find.log 2>&1 &
echo "start finish..."
