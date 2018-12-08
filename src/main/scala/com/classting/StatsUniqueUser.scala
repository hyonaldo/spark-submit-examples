package com.classting

/* StatsUniqueUser.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext

import java.time.ZoneId
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object StatsUniqueUser {
    case class UniqueStats(id: String, role: String, device: String, country: String, unique_cnt: Int, date: String, lang: String, grade: Int, timeStamp:String, _index:String, _type:String)

    val parallelism = 8
    val GS_INPUT_BUCKET = "gs://classting-client-log"
    val GS_OUTPUT_BUCKET = "gs://classting-archive"
    var DATE = "" // very!! very!! very!! important!!
    var lastDays_list = Array(1,7,30)

    def get_names(lastDays: Int, date_str: String) = {
        val year = date_str.substring(0,4)
        val month = date_str.substring(4,6)
        val day = date_str.substring(6,8)

        val todayDir = year + month + day
        val indexName = "unique-stats-" + year
        val typeNameNND = "user" + lastDays + "d-nodev"
        
        val output_path = s"$GS_OUTPUT_BUCKET/$indexName/$typeNameNND/$todayDir"
        
        (indexName, typeNameNND, output_path)
    }

    
    def analysisLog( lastDays:Int, todayDir:String, sc:org.apache.spark.SparkContext, spark:org.apache.spark.sql.SparkSession) {
        import spark.implicits._
        val cal = Calendar.getInstance
        val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    
        cal.setTime(dateFormat.parse(todayDir))
        val timeStamp = cal.getTime
    
        val dummyUrls = List.tabulate(lastDays - 1) { x => //  for test
            cal.add(Calendar.DATE, -1)
            val dummyDir = dateFormat.format(cal.getTime)
            s"$GS_INPUT_BUCKET/logs_" + dummyDir
        }
        val s3Url = dummyUrls ::: List(s"$GS_INPUT_BUCKET/logs_" + todayDir)
        val rowlogsRDD = spark.read.json(s3Url: _*)
        
        //  2016.2.5~
        //      활성화 클래수 정의 : n 개 이상의 글쓰기 활동(포스트 / 사진올리기 / 학급공지만 포함 // 댓글, 읽기, 빛내기 등 활동은 불포함
        //  target_type: class
        //  target_id: class id
        //  resource_type: (texthome, filehome, photohome, videohome, sharehome/photos/notice
        var apiIdx = -1
        var idIdx = -1
        var langIdx = -1
        var deviceIdx = -1
        var tagIdx = -1
        var roleIdx = -1
        var countryIdx = -1
        var codeIdx = -1
        var curIdx = 0
        /*** MODIFIED ***/
        // rowlogsRDD.columns.map  { col =>
        rowlogsRDD.columns.foreach  { col =>
            col match    {
                case "api" => apiIdx = curIdx
                case "id" => idIdx = curIdx
                case "language" => langIdx = curIdx
                case "device" => deviceIdx = curIdx
                case "tag" => tagIdx = curIdx
                case "role" => roleIdx = curIdx
                case "country" => countryIdx = curIdx
                case "code" => codeIdx = curIdx
                case _    =>
            }
            curIdx += 1
        }
        
        //    api=page_move, code=400, id=""
        val activitylogsRDD = rowlogsRDD.rdd.filter { x =>
                !x.isNullAt(apiIdx) &&
                !x.getAs[String](apiIdx).equals("page_move") &&
                !x.getAs[String](apiIdx).equals("_null") &&
                !x.isNullAt(idIdx) &&
                !x.getAs[String](idIdx).isEmpty() &&
                x.getAs[Any](codeIdx) + "" != "400"
        }.coalesce(parallelism, false)
        
        //    for w/o device
        val compactLogsRDD2 = activitylogsRDD.map { x =>
            ((x.getAs[String](idIdx), x.getAs[String](roleIdx), x.getAs[String](countryIdx), x.getAs[String](langIdx)), 1)
        }
        
        val (indexName, typeName, output_path) = get_names(lastDays, todayDir)
        val uniqueLogsDS2 = compactLogsRDD2.reduceByKey(_ + _).map { log =>
            UniqueStats(log._1._1,log._1._2, "_all", log._1._3, log._2, todayDir, log._1._4, 0, timeStamp.toString, indexName, typeName)
        }
        .toDS
        uniqueLogsDS2.coalesce(1).write.option("compression","none").parquet(output_path)
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

        val fs = FileSystem.get(new URI("gs://classting-archive"), sc.hadoopConfiguration)
        date_list.foreach{
            todayDir =>
            lastDays_list.foreach{
                lastDays =>
                val (indexName, typeName, output_path) = get_names(lastDays, todayDir)
                fs.delete(new Path(output_path), true) // isRecusrive= true
                println(s"delete & analysis $lastDays, $todayDir")
                analysisLog( lastDays, todayDir, sc, spark )
               
            }
        }
    }

}

