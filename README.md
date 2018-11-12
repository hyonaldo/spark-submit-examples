# spark maven example
scala code를 maven으로 build하여 submit하는 template

## requirements
* maven 3 이상
* java 1.7 이상

## build
```
mvn clean package
```

## run script (for crontab, etc)
```
$ ./SimpleApp.sh 
Usage: ./SimpleApp.sh [START_DATE] [END_DATE]
e.g) ./SimpleApp.sh 20180901 20180930
```

## SimpleApp.sh includees...
```
$PWD/runner.sh $CLASS "$START_DATE $END_DATE"
```

## ./runner.sh includes ... 
```
/usr/bin/spark-submit --master $MASTER  --class $CLASS \
    --name hkpark/$(basename $CLASS) \
    --num-executors 4 \
    --executor-cores 5 \
    --executor-memory 5G \
    --driver-cores 3 \
    --driver-memory 4G \
$JAR $CLASS_ARGS  2>&1
```

See Also
http://blog.naver.com/gyrbsdl18/220880041737
