package com.classting

/* StatsUniqueClass.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext

import java.time.ZoneId
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.functions._
object StatsUniqueClass {
    case class UniqueStats(id: String, role: String, device: String, country: String, unique_cnt: Long, date: String, lang: String, grade: Long, timeStamp:String, _index:String, _type:String)


    val GS_INPUT_BUCKET = "s3://classting-client-log" //"gs://classting-client-log"
    val GS_OUTPUT_BUCKET = "s3://classting-archive" //"gs://classting-archive"
    val OBJ = "class"
    var DATE = "" // very!! very!! very!! important!!
    var lastDays_list = Array(1,7,30)

    def analysisLog(obj:String, _lastDays:Int, _year:String, _month:String, _day:String, sc:org.apache.spark.SparkContext, spark:org.apache.spark.sql.SparkSession) {
        import spark.implicits._
        val cal = Calendar.getInstance
        val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
        
        val lastDays = _lastDays
        val year = _year
        val month = _month
        val day = _day
        
        val todayDir = year + month + day
        val indexName = "unique-stats-" + year
        val typeNameND = obj + lastDays + "d-nodev"
        
        cal.setTime(dateFormat.parse(todayDir))
        val timeStamp = cal.getTime
        
        val dummyUrls = List.tabulate(lastDays - 1) { x => //  for test
            cal.add(Calendar.DATE, -1)
            val dummyDir = dateFormat.format(cal.getTime)
            s"$GS_INPUT_BUCKET/logs_" + dummyDir
        }
        val s3Url = dummyUrls ::: List(s"$GS_INPUT_BUCKET/logs_" + todayDir)
        val rowlogs_df = spark.read.json(s3Url: _*)
        
        //  2016.2.5~
        //      활성화 클래수 정의 : n 개 이상의 글쓰기 활동(포스트 / 사진올리기 / 학급공지만 포함 // 댓글, 읽기, 빛내기 등 활동은 불포함
        //  target_type: class
        //  target_id: class id
        //  resource_type: (texthome, filehome, photohome, videohome, sharehome/photos/notice
        var targetIdIdx = -1
        var targetTypeIdx = -1
        var resourceTypeIdx = -1
        var tagIdx = -1
        var deviceIdx = -1
        var codeIdx = -1
        var roleIdx = -1
        var langIdx = -1
        var countryIdx = -1
        var curIdx = 0
        /*** MODIFIED ***/
        // rowlogs_df.columns.map  { col =>
        rowlogs_df.columns.foreach  { col =>
            col match   {
                case "target_id" => targetIdIdx = curIdx
                case "target_type" => targetTypeIdx = curIdx
                case "resource_type" => resourceTypeIdx = curIdx
                case "tag" => tagIdx = curIdx
                case "device" => deviceIdx = curIdx
                case "code" => codeIdx = curIdx
                case "language" => langIdx = curIdx
                case "role" => roleIdx = curIdx
                case "country" => countryIdx = curIdx
                case _  =>
            }
            curIdx += 1
        }
        var safe_df = rowlogs_df
        if( rowlogs_df.columns.contains("grade") == false ){
            safe_df = safe_df.withColumn("grade", lit(-999L) )
        }
        if( rowlogs_df.columns.contains("role") == false ){
            safe_df = safe_df.withColumn("role", lit("_null") )
        }
        if( rowlogs_df.columns.contains("device") == false ){
            safe_df = safe_df.withColumn("device", lit("_null") )
        }
        if( rowlogs_df.columns.contains("country") == false ){
            safe_df = safe_df.withColumn("country", lit("_null") )
        }
        if( rowlogs_df.columns.contains("lang") == false ){
            safe_df = safe_df.withColumn("lang", lit("_null") )
        }
        //      println( PREFIX + "targetTypeIdx: " + targetTypeIdx )
        
        val activitylogsRDD = safe_df.filter { x =>
            val targetType = ""+x.getAs[String](targetTypeIdx)
            val resType = ""+x.getAs[String](resourceTypeIdx)
            targetType.equals("class") &&
            (resType.equals("texthome") ||
                resType.equals("filehome") ||
                resType.equals("photohome") ||
                resType.equals("videohome") ||
                resType.equals("sharehome") ||
                resType.equals("photos") ||
                resType.equals("notice") ) &&
            x.getAs[Any]("code") + "" != "400"
        }.
        withColumn("grade", when($"grade".isNull,lit(-999L)).otherwise($"grade")).
        withColumn("language", when($"language".isNull,lit("_null")).otherwise($"language")).
        withColumn("tag", when($"tag".isNull,lit("_null")).otherwise( regexp_replace($"tag", ".event", "") )).
        withColumn("device", when($"device".isNull, $"tag").otherwise( regexp_replace($"device", ".event", "") ))

        //  for w/o device
        /*
        val compactLogsRDD2 = activitylogsRDD.map { x =>
            if( langIdx < 0 )   {
                ((x.getAs[String](targetIdIdx), x.getAs[String](roleIdx), x.getAs[String](countryIdx), "_null"), 1)
            }
            else    {
                ((x.getAs[String](targetIdIdx), x.getAs[String](roleIdx), x.getAs[String](countryIdx), x.getAs[String](langIdx)), 1)
            }
        }

        val uniqueLogsDS2 = compactLogsRDD2.reduceByKey(_ + _).map { log =>
            UniqueStats(log._1._1,log._1._2, "_all", log._1._3, log._2, todayDir, log._1._4, 0, timeStamp.toString, indexName, typeNameND)
        }
        .toDS
        */
        val compactLogsRDD2 = activitylogsRDD.groupBy("target_id", "role", "device", "country", "language", "grade").agg(count("*").alias("unique_cnt"))

        val uniqueLogsDS2 =  compactLogsRDD2.map{
            x =>
            UniqueStats(x.getAs[String]("target_id"), x.getAs[String]("role"), x.getAs[String]("device"), x.getAs[String]("country"), x.getAs[Long]("unique_cnt"), todayDir,
            x.getAs[String]("language"), x.getAs[Any]("grade").toString.toLong,timeStamp.toString, indexName, typeNameND)
        }
        
        uniqueLogsDS2.coalesce(1).write.option("compression","none").parquet(s"$GS_OUTPUT_BUCKET/$indexName/$typeNameND/$todayDir")

    } // end def analysisLog()

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
        val isPrd = 9

        val tmp_dateFormat = DateTimeFormatter.ofPattern("YYYYMMdd")
        val start = LocalDate.of(_year, _month, _day)
        val date_list = (0 to n-1).map{
          i =>
            start.minusDays( i ).toString.replaceAll("-", "")
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
            val year = todayDir.substring(0,4)
            val month = todayDir.substring(4,6)
            val day = todayDir.substring(6,8)
            
            lastDays_list.foreach{
                lastDays =>
                var output_path = s"${GS_OUTPUT_BUCKET}/unique-stats-${year}/class${lastDays}d-nodev/${year}${month}${day}"
                fs.delete(new Path(output_path), true) // isRecusrive= true
                //println(s"delete... $output_path")
                
                println(s"analysisLog( $OBJ, $lastDays, $year, $month, $day, $sc, $spark )" )
                analysisLog( OBJ, lastDays, year, month, day, sc, spark )
                println(java.time.LocalDateTime.now( ZoneId.of( "Asia/Seoul" ) ))
            }
        }
    }

}

