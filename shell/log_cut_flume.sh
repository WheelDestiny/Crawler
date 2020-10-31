#! /bin/bash

# 切割脚本，负责把下载数据，切割到指定的目录

echo '-----------start-----------'
echo 'step1:-----------加载配置文件log_cut_config-----------'
#进到当前脚本所在目录
cd `dirname $0`

#获取当前脚本所在位置
base_path=`pwd`

#通过source 或 . 来加载配置文件参数到当前脚本
. ${base_path}/log_cut_config.sh


echo 'step2:-----------校验配置文件的参数-----------'
#无效参数
invalid=false
if [ "${DATA_PATH}x" == "x" ]; then
	invalid=true
fi

if [ "${DATA_BASE_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_WORK_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_BAK_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_GENERATELOG_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${DATA_HDFS_BASE_PATH}x" == "x" ]; then
	invalid=true
fi
if [ "${LOG_USER}x" == "x" ]; then
	invalid=true
fi

if [ "${invalid}" == "true" ]; then
	echo "log_cut_config.sh params invalid"
	exit
fi 
echo 'step3:-----------校验配置文件中参数是目录的，实际目录存不存在，校验权限-----------'
if [ ! -d $DATA_BASE_PATH ]; then
	mkdir -p $DATA_BASE_PATH
	
	chown ${LOG_USER}:${LOG_USER} $DATA_BASE_PATH
fi

if [ ! -d $DATA_WORK_PATH ]; then
	mkdir -p $DATA_WORK_PATH
	
	chown ${LOG_USER}:${LOG_USER} $DATA_WORK_PATH
fi

if [ ! -d $DATA_BAK_PATH ]; then
	mkdir -p $DATA_BAK_PATH
	
	chown ${LOG_USER}:${LOG_USER} $DATA_BAK_PATH
fi

if [ ! -d $DATA_GENERATELOG_PATH ]; then
	mkdir -p $DATA_GENERATELOG_PATH
	
	chown ${LOG_USER}:${LOG_USER} $DATA_GENERATELOG_PATH
fi

echo 'step4:-----------调用Python脚本把多个小文件合并成大文件-----------'
/usr/local/bin/python /home/wheeldestiny26/hainiu_crawler/Util/html_merge_util.py



ip=`hostname -i`

INPUT_FILES=`ls ${DATA_PATH}/*.done`
for f in $INPUT_FILES
do
	
        timestmap=`date -d 5' mins ago' +%Y%m%d%H%M%S`

        log_file_name=data_${ip}_${timestmap}

        echo 'step5:-----------备份操作-----------'
        mv $f  ${DATA_BAK_PATH}/${log_file_name}.log
        chown ${LOG_USER}:${LOG_USER} ${DATA_BAK_PATH}/${log_file_name}.log	


	echo 'step6:-----------cp操作-----------'
	cp ${DATA_BAK_PATH}/${log_file_name}.log ${DATA_WORK_PATH}


done

echo 'step8:-----------删除2天前的备份数据-----------'
# 今天20190420， 删除的是20190418 的数据
delete_date=`date -d 2' day ago' +%Y%m%d`
rm -f ${DATA_BAK_PATH}/*_${delete_date}*.log

echo '-----------end-----------'






