#!/bin/bash

TARGET_JAR="target/com.classting.jobs-0.1-jar-with-dependencies.jar"
DEPLOY_TO="gs://classting-archive/legacy-codes/"

date
echo "hadoop fs -put -f $TARGET_JAR $DEPLOY_TO"
time hadoop fs -put -f $TARGET_JAR $DEPLOY_TO
hadoop fs -ls $DEPLOY_TO
date
