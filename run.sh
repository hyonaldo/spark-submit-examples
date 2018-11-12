#!/bin/bash

if [[ $# < 1 ]]
then
	echo "Usage: $0 [MainClass] [DATE] [EMAIL]"
	echo "e.g) $0 com.classting.SimpleApp"
	echo "e.g) $0 com.classting.SimpleApp 20161125"
	echo "e.g) $0 com.classting.SimpleApp 20161125 gyrbsdl18@naver.com"
	exit 1
fi

CLASS=$1
DATE=$2
EMAIL=$3
if [[ $DATE == "" ]]
then
	DATE=$(/bin/date -d "3 days ago" +%Y%m%d)
fi
if [[ $EMAIL == "" ]]
then
	EMAIL="gyrbsdl18@naver.com"
fi

PWD=$(cd `dirname $0`; pwd);
JAR=$PWD/target/$CLASS-0.1-jar-with-dependencies.jar
echo "CLASS: $CLASS, JAR: $JAR"

echo $PWD/c3-spark.sh $JAR $CLASS "$DATE $EMAIL"
$PWD/submit.sh $JAR $CLASS "$DATE $EMAIL"
