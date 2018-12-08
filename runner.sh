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
	--num-executors 4 \
	--executor-cores 5 \
	--executor-memory 3G \
	--driver-cores 3 \
	--driver-memory 4G \
"

echo "========================================"
echo "/usr/bin/spark-submit --master $MASTER  --class $CLASS $ARGS $JAR $CLASS_ARGS"
echo "========================================"

/usr/bin/spark-submit --master $MASTER  --class $CLASS \
	--name ${hostname}/$(basename $CLASS) \
	--executor-memory 5G \
	--driver-memory 5G \
$JAR $CLASS_ARGS  2>&1

# See Also
# https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-submit.html
