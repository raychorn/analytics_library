# Usage: bash analytics_cron.sh START_TIME END_TIME EMAIL TITLE DESCRIPTION COLS 
# Example: /bin/bash /home/hadoop/analytics/library/scripts/analytics_cron.sh "" "" "ifong@smithmicro.com" "" "" ""
# crontab sample:
# 42 15 * * * /bin/bash /home/hadoop/analytics/library/scripts/analytics_cron.sh "" "" "SprintMNDTrial@smithmicro.com" "" "" ""

PRE_2_DATE=`date --date='1 days ago' +%Y-%m-%d`
# PRE_2_DATE=`date --date='10 days ago' +%Y-%m-%d`
PRE_2_TIME="06:00:00"
#PRE_2_TIME="22:00:00"
PRE_DATE=`date --date='0 days ago' +%Y-%m-%d`
#PRE_DATE=`date --date='2 days ago' +%Y-%m-%d`
PRE_TIME="05:59:59"
#PRE_TIME="21:59:59"
DATE=`date +%Y-%m-%d`
TIME=`date +%H:%M:%S`
#PRE_FILENAME=events.log_${PRE_DATE}_${TIME}
#FILENAME=events.log_${DATE}_${TIME}
#echo $PRE_FILENAME 
#echo $FILENAME
#echo "Test $FILENAME"

HOST="10.100.162.180"
PORT="8082"

START_TIME=$1
if [ -z $START_TIME ]
then
START_TIME="${PRE_2_DATE} ${PRE_2_TIME}"
fi

END_TIME=$2
if [ -z $END_TIME ]
then
END_TIME="${PRE_DATE} ${PRE_TIME}"
fi

EMAIL=$3
if [ -z $EMAIL ]
then
EMAIL="analytics@smithmicro.com"
fi

TITLE=$4
if [ -z $TITLE ]
then
TITLE="Analytics_${DATE}_${TIME}"
fi

DESCRIPTION=$5
if [ -z $DESCRIPTION ]
then
DESCRIPTION="testing"
fi

COLS=$6
if [ -z $COLS ]
then
COLS="uuid,en,152,client_date,17,47,48,53,141,142,159"
fi

# source /home/hadoop/.profile; /usr/bin/curl "http://10.100.162.180:8080/admin/data" -d "start_date=${PRE_DATE} ${PRE_TIME}&end_date=${DATE} ${TIME}&email=ifong@smithmicro.com&title=Analytics_${DATE}_${TIME}&description=testing&columns=uuid,en,152,client_date,17,47,48,53,141,142,159&c_48=1.5"
URL="http://${HOST}:${PORT}/admin/data"
PARAM="start_date=${START_TIME}&end_date=${END_TIME}&email=${EMAIL}&title=${TITLE}&description=${DESCRIPTION}&israw=True&scp=True&target_user=root&target_server=10.1.3.120&target_dir=%2Fhome%2Foracle%2FSprintMND%2Fstaging%2F&columns=${COLS}&c_48=1.5"

#echo $URL
#echo $PARAM

source /home/hadoop/.profile; /usr/bin/curl "${URL}" -d "${PARAM}"

# scp data into the Oracle server
#scp /home/${EMAIL}/csv/${TITLE}.csv root@10.1.3.120:/home/oracle/SprintMND/staging/.
