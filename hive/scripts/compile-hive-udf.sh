#!/bin/bash
# Usage: bash compile-hive-udf.sh <java file>
if [ "$1" == "" ]; then
   echo "Usage: $0 <java file>"
   exit 1
fi

CNAME=${1%.java}
JARNAME=$CNAME.jar
JARDIR=/tmp/hive_jars/$CNAME
CLASSPATH=$(ls $HIVE_HOME/lib/hive-serde-*.jar):$(ls $HIVE_HOME/lib/hive-exec-*.jar):$(ls $HADOOP_HOME/hadoop-core-*.jar):$(ls $HADOOP_HOME/hadoop-core.jar)

function tell {
    echo
    echo "$1 successfully compiled.  In Hive run:"
    echo "$> add jar /home/hadoop/analytics/hive/lib/$JARNAME;"
    echo "$> create temporary function $CNAME as 'com.smithmicro.hive.udf.$CNAME';"
    echo
}

rm -r $JARDIR   # clean old class directory
mkdir -p $JARDIR
javac -classpath $CLASSPATH -d $JARDIR/ $1 && jar -cf $JARNAME -C $JARDIR/ . && tell $1
mv $JARNAME $HIVE_HOME/lib/
