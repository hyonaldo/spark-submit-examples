#!/bin/bash
source ~/.bashrc

PWD=$(cd `dirname $0`; pwd);

if [ $# -ne 3 ]
then
	echo "Usage: $0 [FROM] [TO] [COMMAND]"
	echo "$0 20170329 20170321 ./StatsAccumClass.sh"
	exit 1
fi

YESTER=`/bin/date +%Y%m%d -d "1 day ago"`
START=`date +%Y%m%d -d $1`
END=`date +%Y%m%d -d $2`
CMD=$3

INC=1
if [ $START -gt $END ]
then
	INC=-1
fi

echo $CMD $START 1
time $CMD $START 1

CURRENT=$START
while [ $CURRENT -ne $END ]
do
	CURRENT=$(/bin/date +%Y%m%d -d "$CURRENT $INC day")
	echo $CMD $CURRENT 1
	time $CMD $CURRENT 1
done
