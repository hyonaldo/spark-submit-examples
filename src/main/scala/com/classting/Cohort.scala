package com.classting

/* Cohort.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import java.time.ZoneId
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Cohort {
    case class CohortLog(signup: Long, date: String, lastMonths: Int, submit: String, timestamp: String,
        _index: String, _type: String, last1M: Double, last1M_cnt: Long)
    val parallelism = 2000
    val lastMonths = 1

    val GS_INPUT_BUCKET = "gs://classting-client-log"
    val GS_OUTPUT_BUCKET = "gs://classting-archive"
    var DATE = "" // very!! very!! very!! important!!
    def get_names(date_str: String) = {
        val year = date_str.substring(0,4)
        val month = date_str.substring(4,6)
        val day = date_str.substring(6,8)

        val indexName = "cohort-" + year
        var sMonthStr = month
        val sMonth = month.toInt - lastMonths - 1
        if( sMonth < 10 ) {
            sMonthStr = "0"+sMonth
        }
        else    {
            sMonthStr = sMonth.toString
        }
        val typeName = year + sMonthStr
        val output_path = s"$GS_OUTPUT_BUCKET/$indexName/$typeName/$date_str"
        (indexName, typeName, output_path)
    }

    def analysisLog(todayDir:String, sc:org.apache.spark.SparkContext, spark:org.apache.spark.sql.SparkSession) {
        import spark.implicits._
        val cal = Calendar.getInstance
        val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    
        cal.setTime(dateFormat.parse(todayDir))
        val timeStamp = cal.getTime
        
        val (indexName, typeName, output_path) = get_names(todayDir)
    
        ///////////////////////////////////
        // for activity
        val tmpDate = dateFormat.format(cal.getTime)
        val tmpDF = spark.sqlContext.read.json(s"$GS_INPUT_BUCKET/logs_" + tmpDate + "/" + tmpDate + "-0*_0_all_*.gz")//20181009-01_0_all_a.gz
        var activityRDDs = Map[Int, RDD[(String, Int)]]()
        val urls = List.tabulate(30) { x =>
                cal.add(Calendar.DATE, -1)
                val date = dateFormat.format(cal.getTime)
                s"$GS_INPUT_BUCKET/logs_" + date + "/*"
            }
    
        val rawlogsDF = spark.sqlContext.read.schema(tmpDF.schema).json(
            (for (
                path <- urls
            ) yield sc.textFile(path)).reduce(_ union _)
        )

        val filteredActivityRDD = rawlogsDF
        .filter(rawlogsDF("api").isNotNull && rawlogsDF("api").notEqual("page_move") && rawlogsDF("api").notEqual("_null") && rawlogsDF("id").isNotNull)
        .rdd
        .coalesce(parallelism, false)

        val activityPairRDD = filteredActivityRDD.map(x => (x.getAs[String]("id"), 1)).reduceByKey(_+_).mapValues(v=>1)
        println( "activityPairRDD: " + activityPairRDD.count() )
        activityRDDs += (1 -> activityPairRDD)
        
        val signupUrls = List.tabulate(30) { x =>
            cal.add(Calendar.DATE, -1)
            val date = dateFormat.format(cal.getTime)
            s"$GS_INPUT_BUCKET/logs_" + date + "/*"
        }
    
        var _apiIdx = -1
        var _methodIdx = -1
        var _langIdx = -1
        var _deviceIdx = -1
        var _tagIdx = -1
        var _roleIdx = -1
        var _countryIdx = -1
        var _codeIdx = -1
        var _idIdx = -1
    
        var _curIdx = 0
    
        rawlogsDF.columns.foreach    { col =>
            col match    {
                case "id" => _idIdx = _curIdx
                case "api" => _apiIdx = _curIdx
                case "method" => _methodIdx = _curIdx
                case "code" => _codeIdx = _curIdx
                case "language" => _langIdx = _curIdx
                case "device" => _deviceIdx = _curIdx
                case "tag" => _tagIdx = _curIdx
                case "role" => _roleIdx = _curIdx
                case "country" => _countryIdx = _curIdx
                case _    =>
            }
            _curIdx += 1
        }
    
        // val filteredSignupRDD = signupDF.filter    { x =>
        val filteredSignupRDD = rawlogsDF.filter    { x =>
            !x.isNullAt(_idIdx) &&
            !x.isNullAt(_apiIdx) &&
                ( x.getAs[String](_apiIdx) == "https://www.classting.com/api/users" ||
                    x.getAs[String](_apiIdx) == "https://oauth.classting.com/v1/oauth2/sign_up" ||
                    x.getAs[String](_apiIdx) == "https://oauth.classting.com/api/users" )&&
                x.getAs[String](_methodIdx) == "POST" &&
                x.getAs[Any](_codeIdx) + "" == "200"
        }
        .rdd
        .coalesce(parallelism, false)
        
        val signUpPairRDD = filteredSignupRDD.map(x => (x.getAs[String]("id"), 1)).reduceByKey(_+_).mapValues(v=>1)
        val signupCnt = signUpPairRDD.count()

        val last = lastMonths
        val signOutPairRDD = signUpPairRDD.subtract( activityRDDs(last) )
        val remainCnt = signUpPairRDD.count() - signOutPairRDD.count()
        val remainRaw = ((remainCnt.toFloat/signupCnt.toFloat) * 100)
        val remain = (remainRaw*1000).round / 1000.toDouble
        // sc.makeRDD(Seq(cohort)).saveToEs(indexName+"/"+typeName, Map("es.nodes" -> esNode))        
        Seq(
            CohortLog(signupCnt, todayDir, lastMonths, timeStamp.toString, timeStamp.toString,
            indexName, typeName, remain, remainCnt)
        )
        .toDS
        .write.option("compression","none").parquet(output_path)
        
        println(output_path)
        println(java.time.LocalDateTime.now( ZoneId.of( "Asia/Seoul" ) ))
        rawlogsDF.unpersist()
    }

    def main(args: Array[String]) {
        val usage = s"""
        Usage: [START_DATE] [NUM_DAYS]
        """
        var argString = ""

        if (args.size != 2){
            argString = args.mkString(" , ")
            println(s"""
            Error ${args.size} Args: ${argString}
            ${usage}
            """)
            System.exit(1)
        }
        val START_DATE = args(0)
        val NUM_DAYS = args(1)

        println(s"""
        ----------------------------
        START_DATE: ${START_DATE}
        NUM_DAYS: ${NUM_DAYS}
        ----------------------------
        """)

        var _date =  START_DATE.toString
        val _year = _date.substring(0,4).toInt
        val _month = _date.substring(4,6).toInt
        val _day = _date.substring(6,8).toInt

        var n = NUM_DAYS.toString.toDouble.toInt

        val tmp_dateFormat = DateTimeFormatter.ofPattern("YYYYMMdd")
        val start = LocalDate.of(_year, _month, _day)
        val date_list = (0 to n-1).map{
            i =>
            // bypass java bug
            if( DATE.endsWith("0101") ){
                val year = DATE.substring(0,4).toInt
                    DATE = (year - 1).toString + "1231"
            }else{
                DATE = start.minusDays( i ).format(tmp_dateFormat)
            }
            DATE
        }

        val spark = SparkSession.builder()
        .config("spark.ui.showConsoleProgress", false)
        .getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        val fs = FileSystem.get(new URI(GS_OUTPUT_BUCKET), sc.hadoopConfiguration)
        date_list.foreach{
            todayDir =>
            val (indexName, typeName, output_path) = get_names(todayDir)
            println(output_path)
            
            fs.delete(new Path(output_path), true) // isRecusrive= true
            println(s"delete... $output_path")

            analysisLog(todayDir, sc, spark )
        }
    }

}

