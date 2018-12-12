#!/bin/bash

BASENAME=$(basename $0 .sh)

if [[ $# < 2 ]]
then
	echo "Usage: $0 [START_DATE] [NUM_DAYS]"
	echo "e.g) $0 20180831 31"
	exit 1
fi

CLASS=com.classting.$BASENAME
START_DATE=$1
NUM_DAYS=$2

PWD=$(cd `dirname $0`; pwd);

echo $PWD/runner.sh $CLASS "$START_DATE $NUM_DAYS"
$PWD/runner.sh $CLASS "$START_DATE $NUM_DAYS"
