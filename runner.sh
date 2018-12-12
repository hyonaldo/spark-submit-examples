#!/bin/bash

JAR="target/com.classting.jobs-0.1-jar-with-dependencies.jar"
CLASS=$1
CLASS_ARGS=$2

if [[ $# != 2 ]]
then
	echo "Usage: $0 [CLASS] [CLASS_ARGS]"
	echo "e.g. $0 com.classting.SimpleApp '20180901 20180930'"
	exit 1
fi


MASTER=yarn
ARGS=" \
	--name ${hostname}/$(basename $CLASS) \
	--num-executors 8 \
	--executor-cores 2 \
	--executor-memory 3G \
	--driver-cores 2 \
	--driver-memory 3G \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.eventLog.enabled=false \
"

echo "========================================"
echo "/usr/bin/spark-submit --master $MASTER  --class $CLASS $ARGS $JAR $CLASS_ARGS"
echo "========================================"

/usr/bin/spark-submit --master $MASTER  --class $CLASS \
	--name ${hostname}/$(basename $CLASS) \
	--num-executors 8 \
	--executor-cores 2 \
	--executor-memory 3G \
	--driver-cores 2 \
	--driver-memory 3G \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.eventLog.enabled=false \
$JAR $CLASS_ARGS  2>&1

# See Also
# https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-submit.html
