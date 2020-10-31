#!/bin/sh
echo "start news download action"
nohup /usr/local/bin/python /home/wheeldestiny26/hainiu_crawler/Download_page/download_news_action.py > /home/wheeldestiny26/python/hainiu_crawler/shell/news_download.log 2>&1 &
echo "start finish..."