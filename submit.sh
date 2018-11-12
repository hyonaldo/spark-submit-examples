#!/bin/bash

JAR=$1
CLASS=$2
CLASS_ARGS=$3

if [[ $# != 3 ]]
then
	echo "Usage: $0 [JAR] [CLASS] [CLASS_ARGS]"
	echo "e.g. $0 target/com.classting.SimpleApp-0.1-jar-with-dependencies.jar com.classting.SimpleApp '20161125 gyrbsdl18@naver.com'"
	exit 1
fi


MASTER=yarn
ARGS=" \
	--queue dev --name hkpark/$(basename $CLASS) \
	--num-executors 4 \
	--executor-cores 5 \
	--executor-memory 3G \
	--driver-cores 3 \
	--driver-memory 4G \
"

echo "========================================"
echo "$SPARK_HOME/bin/spark-submit --master $MASTER  --class $CLASS $ARGS $JAR $CLASS_ARGS"
echo "========================================"

$SPARK_HOME/bin/spark-submit --master $MASTER  --class $CLASS \
	--queue dev --name hkpark/$(basename $CLASS) \
	--num-executors 4 \
	--executor-cores 5 \
	--executor-memory 5G \
	--driver-cores 3 \
	--driver-memory 4G \
$JAR $CLASS_ARGS  2>&1

# See Also
# https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-submit.html
