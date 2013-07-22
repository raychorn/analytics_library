#!/bin/bash
TIME=`date +%Y-%m-%d-%H:%M`
declare -a JOBS
let count=0
exec 3< ${ANALYTICS_LIB}/python_streaming/tmp_dates_and_events.txt
while read <&3
do 
 JOBS[$count]="$REPLY""\n"
 echo "start:${REPLY// /t}:end"
 IFS="}"; declare -a Array=($REPLY) 
 echo "start:${Array[0]}:end"
 echo "start:${Array[1]}:end"
 ((count++))
done
exec 3>&-
echo ${TIME}
echo '$ANALYTICS_LIB'
echo ${HADOOP_HOME}
echo ${HIVE_HOME}
echo ${HOSTNAME}
#RESULT=$(hadoop dfs -ls /user/hadoop/sprintcm/`date +%Y%m%d`*) || { echo "command failed"; exit 1; } 
#RESULT=$(hadoop dfs -ls /user/hadoop/sprintcm/test*) || { echo "command failed"; exit 1; }
#echo $RESULT
S_ARRAY="${JOBS[@]}"
REQUESTID="hdfs-time-hdfs--`date +%Y%m%d%H%M%S`"
hadoop dfs -mkdir /user/hadoop/sprintcm/${REQUESTID}
#python ${ANALYTICS_LIB}/scripts/hadoop_mailer.py "bashtest" complete hive3 lramos@smithmicro.com " Processed the following Log Files:\n${RESULT}\n\n ${S_ARRAY}"
#hive --config ${HIVE_HOME}/conf -e "CREATE TABLE IF NOT EXISTS InstallationInfo (server_date STRING, ip STRING, log_format STRING, client_date STRING, project_id STRING, version STRING, uuid STRING, xvi STRING, xvii STRING, ii STRING, xlii STRING, xliii STRING, xliv STRING) PARTITIONED BY(ds STRING, ts STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;"
