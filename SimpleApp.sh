#!/bin/bash

BASENAME=$(basename $0 .sh)

if [[ $# < 2 ]]
then
	echo "Usage: $0 [START_DATE] [END_DATE]"
	echo "e.g) $0 20180901 20180930"
	exit 1
fi

CLASS=com.classting.$BASENAME
START_DATE=$1
END_DATE=$2

PWD=$(cd `dirname $0`; pwd);

echo $PWD/runner.sh $CLASS "$START_DATE $END_DATE"
$PWD/runner.sh $CLASS "$START_DATE $END_DATE"
