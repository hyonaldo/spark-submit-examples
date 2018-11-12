# spark maven example
scala code를 maven으로 build하여 c3에 submit하는 template

## requirements
* maven 3 이상
* java 1.7 이상
* c3 개발 환경 구성
 * http://c3.doc.nhnsystem.com/docs/c3-user/en/latest/dev_env.html
* sendmail 설정
 * http://blog.naver.com/gyrbsdl18/220868516474
 * example이 spark conf 를 메일을 보내주는 내용이라서 필요
 * 메일 보내는 부분을 실행하기 싫다면 코드를 수정하면 됨  

## build
```
mvn clean package
```

## run script (for crontab, etc)
```
$ ./run.sh
Usage: ./run.sh [MainClass] [DATE] [EMAIL]
e.g. ./run.sh com.navercorp.SimpleApp
e.g. ./run.sh com.navercorp.SimpleApp 20161125
e.g. ./run.sh com.navercorp.SimpleApp 20161125 hyokyun.park@navercorp.com
```

## run script includees...
```
$ ./c3-spark.sh target/com.navercorp.SimpleApp-0.1-jar-with-dependencies.jar com.navercorp.SimpleApp '20161125 hyokyun.park@navercorp.com'
```

## ./c3-spark.sh includes ... 
```

$SPARK_HOME/bin/spark-submit --master $MASTER  --class $CLASS \
        --queue large --name hkpark/$(basename $CLASS) \
        --num-executors 32 \
        --executor-cores 5 \ 
        --executor-memory 8G \
        --driver-cores 5 \ 
        --driver-memory 8G \
$JAR $CLASS_ARGS  2>&1

```

See Also
http://blog.naver.com/gyrbsdl18/220880041737
