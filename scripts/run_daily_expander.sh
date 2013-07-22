#!/bin/bash
# run_daily_expander.sh
#
# Driver script for running daily aggregations
# and monthly trend estimation. 
# 
#
# Usage:
#
# $ bash analytics-thirdparty/lib/scripts/run_daily_timelines.sh LOGFILE GROUPID REQUESTID MAILTO
#
# To clean the output directories before running again:
#
# $ hadoop dfs -rmr stage1-output
# $ hadoop dfs -rmr tmp/
#

# Check for ANALYTICS_LIB environment variable
# if it doesn't exist try looking for it in the user's profile
# if there is nothing after that it should quit, the variable
# should point to the location of the analytics_library project.
if [ -z $ANALYTICS_LIB ]
then
source /etc/profile.d/abundle_profile.sh
source /home/hadoop/.profile
fi

if [ -z $ANALYTICS_LIB ]
then
echo "ANALYTICS_LIB Env Variable Required, look at notes."
exit 1
fi

source /etc/analytics/expander.properties
if [ -z $parallel_processes ]
then
parallel_processes=2
fi

if [ $# -lt 2 ]
then
        echo "at least pass the first two arguments expected, did you pass in the 1.(hdfs log file, ie:events.log), 2.(Group ID, ie: dev,vzmm,warehouse,bakrie), and 3.(RequestID, ie: logger1_rotate, hdfs_test)?"
	echo "Example 1: bash run_daily_expander.sh \"*.log*\" sprintcm \"Request-Test-2010\" \"my@email.com\" Example 2: bash run_daily_expander.sh events.log bakrie "" analyticsBackend@smithmicro.com"
        exit 1
fi
LOGFILE=$1
GROUPID=$2
REQUESTID=$3
MAILTO=$4

# Check all inputs before processing them
echo "-- Start run_daily_expander.sh --------------------------------------"
echo "LOGFILE: ${LOGFILE}"
echo "GROUPID: ${GROUPID}"
echo "REQUESTID: ${REQUESTID}"
echo "MAILTO: ${MAILTO}"
echo "PARALLEL EXPANDERS: $parallel_processes"
echo "---------------------------------------------------------------------"

# Check for REQUESTID if NULL add Default Value
DATETIMEID=`date +%Y%m%d_%H%M%S`
if [ -z $REQUESTID ]
then
REQUESTID="hdfs"
fi
REQUESTID="${REQUESTID}_$DATETIMEID"
echo "REQUESTID: ${REQUESTID}"

# Check for MAILTO if NULL add Default Value
if [ -z $MAILTO ]
then
MAILTO="Analytics.Notifications@smithmicro.com"
fi
echo "MAILTO: ${MAILTO}"

# Check that the log files exist and assign the results, quit of no logs exists.
HDFS_LOGS=$(hadoop dfs -ls /user/hadoop/${GROUPID}/${LOGFILE}) || { echo "Log File was not Found, will not proceed."; python ${ANALYTICS_LIB}/scripts/hadoop_mailer.py "${GROUPID}/${LOGFILE}" empty "analytics.smithmicro.com" $MAILTO "\nError: Log File was not Found in HDFS"; exit 1; }

#Prepare the Log Files for processing by creating a unique location based on the requestid to allow new logs to come in.
echo "--Creating Processing folder: /user/hadoop/${GROUPID}/${REQUESTID}"
hadoop dfs -mkdir /user/hadoop/${GROUPID}/${REQUESTID}/ &
sleep 5
hadoop dfs -mv /user/hadoop/${GROUPID}/${LOGFILE} /user/hadoop/${GROUPID}/${REQUESTID}/
echo "--Creating Processed folder: /user/hadoop/${GROUPID}/processed"
HDFS_LOGS=$(hadoop dfs -mkdir /user/hadoop/${GROUPID}/processed) || { echo "/user/hadoop/${GROUPID}/processed already exists"; }
sleep 3

# TODO: make the parameters configurable 
# TODO: convert to a rake task

# create hive databases and tables if not exist
hive -hiveconf hive.metastore.warehouse.dir=/user/hive/${GROUPID} -e "CREATE DATABASE IF NOT EXISTS ${GROUPID}"
if [ $GROUPID == "SprintMND" ]
then
        hive -hiveconf hive.metastore.warehouse.dir=/user/hive/${GROUPID} -e "USE ${GROUPID}; CREATE TABLE IF NOT EXISTS LogEvents (server_date STRING, ip STRING, log_format INT, client_date STRING, project_id STRING, version STRING, uuid STRING, c_1 STRING, c_10 STRING, c_11 INT, c_12 STRING, c_13 INT, c_137 INT, c_138 STRING, c_139 DOUBLE, c_14 STRING, c_140 DOUBLE, c_141 INT, c_142 INT, c_143 INT, c_144 INT, c_145 INT, c_146 INT, c_147 INT, c_148 STRING, c_149 STRING, c_15 INT, c_150 INT, c_151 INT, c_152 STRING, c_156 INT, c_157 STRING, c_158_1 INT, c_158_2 INT, c_158_3 INT, c_158_4 INT, c_159 INT, c_16 INT, c_160_1 STRING, c_160_2 STRING, c_160_3 STRING, c_160_4 STRING, c_161_1 STRING, c_161_2 STRING, c_161_3 STRING, c_161_4 STRING, c_162_1 INT, c_162_2 INT, c_162_3 INT, c_162_4 INT, c_163_1 INT, c_163_2 INT, c_163_3 INT, c_163_4 INT, c_164 INT, c_165 INT, c_166 DOUBLE, c_167 DOUBLE, c_168 DOUBLE, c_169 DOUBLE, c_17 INT, c_170_1 STRING, c_170_2 STRING, c_170_3 STRING, c_170_4 STRING, c_171_1 DOUBLE, c_171_2 DOUBLE, c_171_3 DOUBLE, c_171_4 DOUBLE, c_172_1 DOUBLE, c_172_2 DOUBLE, c_172_3 DOUBLE, c_172_4 DOUBLE, c_173_1 STRING, c_173_2 STRING, c_173_3 STRING, c_173_4 STRING, c_174 INT, c_175 INT, c_176 STRING, c_177 STRING, c_18 INT, c_19 STRING, c_191 STRING, c_192 STRING, c_193 STRING, c_194 STRING, c_195 STRING, c_196 STRING, c_2 STRING, c_20 INT, c_200_1 INT, c_200_2 INT, c_200_3 INT, c_200_4 INT, c_201 STRING, c_202 STRING, c_203 STRING, c_204 STRING, c_205 INT, c_206 INT, c_207 INT, c_208 INT, c_209 INT, c_21 INT, c_210 STRING, c_211 INT, c_22 INT, c_23 INT, c_24 INT, c_25 INT, c_26 STRING, c_27 STRING, c_28 INT, c_29 STRING, c_3 STRING, c_30 STRING, c_31 STRING, c_32 STRING, c_33 STRING, c_34 STRING, c_35 STRING, c_36 STRING, c_37 STRING, c_38 STRING, c_39 STRING, c_4 STRING, c_40 STRING, c_41 STRING, c_42 STRING, c_43 STRING, c_44 INT, c_45 STRING, c_46 STRING, c_47 STRING, c_48 STRING, c_49 STRING, c_5 INT, c_50 INT, c_51 STRING, c_52 STRING, c_53 STRING, c_54 INT, c_55 INT, c_56 INT, c_57 INT, c_58 STRING, c_59 STRING, c_6 STRING, c_60 INT, c_61 INT, c_62 INT, c_63 INT, c_64 STRING, c_65 STRING, c_66 INT, c_67 INT, c_68 INT, c_69 STRING, c_7 STRING, c_70 STRING, c_71 STRING, c_72 STRING, c_73 STRING, c_74 STRING, c_75 INT, c_76 INT, c_77 INT, c_78 INT, c_79 STRING, c_8 INT, c_80 INT, c_81 INT, c_82 INT, c_83 INT, c_84 INT, c_85 INT, c_86 INT, c_87 INT, c_88 INT, c_89 STRING, c_9 STRING, c_90 INT, c_91 STRING, bytemobile STRING, device STRING, devicetype STRING, heat_lat INT, heat_lng INT, heat_x INT, heat_y INT, heat_num INT, heat_gps STRING) PARTITIONED BY(ds STRING, en STRING, ts STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;"
else
        hive -hiveconf hive.metastore.warehouse.dir=/user/hive/${GROUPID} -e "USE ${GROUPID}; CREATE TABLE IF NOT EXISTS LogEvents (server_date STRING, ip STRING, log_format INT, client_date STRING, project_id STRING, version STRING, uuid STRING, c_1 STRING, c_10 STRING, c_11 INT, c_12 STRING, c_13 INT, c_137 INT, c_138 STRING, c_139 DOUBLE, c_14 STRING, c_140 DOUBLE, c_141 INT, c_142 INT, c_143 INT, c_144 INT, c_145 INT, c_146 INT, c_147 INT, c_148 STRING, c_149 STRING, c_15 INT, c_150 INT, c_151 INT, c_152 STRING, c_156 INT, c_157 STRING, c_158_1 INT, c_158_2 INT, c_158_3 INT, c_158_4 INT, c_159 INT, c_16 INT, c_160_1 STRING, c_160_2 STRING, c_160_3 STRING, c_160_4 STRING, c_161_1 STRING, c_161_2 STRING, c_161_3 STRING, c_161_4 STRING, c_162_1 INT, c_162_2 INT, c_162_3 INT, c_162_4 INT, c_163_1 INT, c_163_2 INT, c_163_3 INT, c_163_4 INT, c_164 INT, c_165 INT, c_166 DOUBLE, c_167 DOUBLE, c_168 DOUBLE, c_169 DOUBLE, c_17 INT, c_170_1 STRING, c_170_2 STRING, c_170_3 STRING, c_170_4 STRING, c_171_1 DOUBLE, c_171_2 DOUBLE, c_171_3 DOUBLE, c_171_4 DOUBLE, c_172_1 DOUBLE, c_172_2 DOUBLE, c_172_3 DOUBLE, c_172_4 DOUBLE, c_173_1 STRING, c_173_2 STRING, c_173_3 STRING, c_173_4 STRING, c_174 INT, c_175 INT, c_176 STRING, c_177 STRING, c_18 INT, c_19 STRING, c_191 STRING, c_192 STRING, c_193 STRING, c_194 STRING, c_195 STRING, c_196 STRING, c_2 STRING, c_20 INT, c_200_1 INT, c_200_2 INT, c_200_3 INT, c_200_4 INT, c_201 STRING, c_202 STRING, c_203 STRING, c_204 STRING, c_205 INT, c_206 INT, c_207 INT, c_208 INT, c_209 INT, c_21 INT, c_210 STRING, c_211 INT, c_22 INT, c_23 INT, c_24 INT, c_25 INT, c_26 STRING, c_27 STRING, c_28 INT, c_29 STRING, c_3 STRING, c_30 STRING, c_31 STRING, c_32 STRING, c_33 STRING, c_34 STRING, c_35 STRING, c_36 STRING, c_37 STRING, c_38 STRING, c_39 STRING, c_4 STRING, c_40 STRING, c_41 STRING, c_42 STRING, c_43 STRING, c_44 INT, c_45 STRING, c_46 STRING, c_47 STRING, c_48 STRING, c_49 STRING, c_5 INT, c_50 INT, c_51 STRING, c_52 STRING, c_53 STRING, c_54 INT, c_55 INT, c_56 INT, c_57 INT, c_58 STRING, c_59 STRING, c_6 STRING, c_60 INT, c_61 INT, c_62 INT, c_63 INT, c_64 STRING, c_65 STRING, c_66 INT, c_67 INT, c_68 INT, c_69 STRING, c_7 STRING, c_70 STRING, c_71 STRING, c_72 STRING, c_73 STRING, c_74 STRING, c_75 INT, c_76 INT, c_77 INT, c_78 INT, c_79 STRING, c_8 INT, c_80 INT, c_81 INT, c_82 INT, c_83 INT, c_84 INT, c_85 INT, c_86 INT, c_87 INT, c_88 INT, c_89 STRING, c_9 STRING, c_90 INT, c_91 STRING, bytemobile STRING, device STRING, devicetype STRING, heat_lat INT, heat_lng INT, heat_x INT, heat_y INT, heat_num INT, heat_gps STRING) PARTITIONED BY(ds STRING, en STRING, ts STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;"
fi

#Email user that Hadoop Job is STARTING
echo "===>Logfile = ${LOGFILE}; GROUPID=${GROUPID};REQUESTID=${REQUESTID};MAILTO=${MAILTO}"
python ${ANALYTICS_LIB}/scripts/hadoop_mailer.py "${GROUPID}/${LOGFILE}-${REQUESTID}" starting "analytics.smithmicro.com" $MAILTO "\nLog Files to be Processed:\n${HDFS_LOGS}"

# start the hadoop streaming jobs
echo "--- Starting Hadoop Job Current Log is: ${GROUPID}/${LOGFILE} ---"
hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop*streaming*jar \
  -D mapred.job.name=${GROUPID}/${LOGFILE}_daily_expander_stage1 \
  -D mapred.compress.map.output=true \
  -cacheFile "/cache/geotagger#geotagger" \
  -input /user/hadoop/${GROUPID}/${REQUESTID}/* \
  -output tmp/${REQUESTID}-stage1-output \
  -mapper "daily_expander.py mapper1" \
  -reducer "daily_expander.py reducer1" \
  -file "${ANALYTICS_LIB}/python_streaming/daily_expander.py"
  #\ #-jobconf mapred.reduce.tasks=8 \
  #\ #-jobconf mapred.map.tasks=40 \
  #\ #-cacheFile "cache/pbkdf2.py#pbkdf2.py" \
echo "--- End Hadoop Job Current Log is: ${GROUPID}/${LOGFILE} ---"

# output last results into tmp_dates_and_events.txt
hadoop dfs -getmerge tmp/${REQUESTID}-stage1-output /tmp/${REQUESTID}-tmp_dates_and_events.txt
# clean output from hadoop (trailing tab/spaces)
sed 's/[ \t]*$//' /tmp/${REQUESTID}-tmp_dates_and_events.txt > /tmp/${REQUESTID}-tmp_dates_and_events_clean.txt


# start second stage of hadoop streaming jobs
#

# Function holding the second stage of the streaming job
let count=0
let maxjobs=$parallel_processes
let jobsrunning=0
job (){
 IFS="}"; declare -a Array=($1)
 TIME=`date +%Y-%m-%d-%H:%M:%S`
 echo "---Start: ${Array[0]}-${Array[1]}"
 sleep 1
 hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop*streaming*jar \
  -D mapred.job.name=daily_expander_stage2_${GROUPID}/${LOGFILE}/${REQUESTID}-${Array[0]}-${Array[1]} \
  -D mapred.compress.map.output=true \
  -D mapred.reduce.tasks=0 \
  -cacheFile "/cache/geotagger#geotagger" \
  -input /user/hadoop/${GROUPID}/${REQUESTID}/* \
  -output tmp/${REQUESTID}-${Array[0]}-${Array[1]} \
  -mapper "daily_expander.py mapper2 $1 ${ANALYTICS_LIB}/events/${GROUPID}/logevents" \
  -file "${ANALYTICS_LIB}/python_streaming/daily_expander.py" \
  -numReduceTasks 0
  #\ #-reducer "daily_expander.py reducer2 $1 ${ANALYTICS_LIB}/events/${GROUPID}/logevents" \
  #\ #-jobconf mapred.reduce.tasks=8 \
  #\ #-jobconf mapred.map.tasks=40 \
 hadoop dfs -rmr tmp/${REQUESTID}-${Array[0]}-${Array[1]}/_logs
 hive --config ${HIVE_HOME}/conf -hiveconf hive.metastore.warehouse.dir=/user/hive/${GROUPID} -e "USE ${GROUPID}; LOAD DATA INPATH 'tmp/${REQUESTID}-${Array[0]}-${Array[1]}' INTO TABLE LogEvents PARTITION (ds='${Array[0]}',en='${Array[1]}',ts='${TIME}');"
 echo "---Done: ${Array[0]}-${Array[1]}"
}
# Declare array that will hold content of event/date (Job) file to send to email
declare -a JOBS
# open file to find the combinations of dates and events
exec 3< /tmp/${REQUESTID}-tmp_dates_and_events_clean.txt
while read <&3
do
 #echo "REPLY: $REPLY"
 JOBS[$count]="${REPLY}""\n"
 job $REPLY &
 ((jobsrunning++))
 if [[ jobsrunning -ge maxjobs ]]; then
  wait
  let jobsrunning=0
 fi
 ((count++))
done
exec 3>&-
wait

#Moved the completed log files into the processed folder and delete the created folder for the requestid.
hadoop dfs -mv /user/hadoop/${GROUPID}/${REQUESTID}/ /user/hadoop/${GROUPID}/processed/

JOB_LIST="${JOBS[@]}"
python ${ANALYTICS_LIB}/scripts/hadoop_mailer.py "${GROUPID}/${LOGFILE}-${REQUESTID}" complete "analytics.smithmicro.com" $MAILTO "\nProcessed Jobs for Analytics (dates/events found):\n $JOB_LIST" 

# remove trash files
#rm /tmp/${REQUESTID}*tmp_dates_and_events*
hadoop dfs -rmr tmp/${REQUESTID}*

echo "-- End run_daily_expander.sh --------------------------------------"
