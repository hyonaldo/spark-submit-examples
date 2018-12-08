package com.classting

/* StatsAccumUser.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext

import java.time.ZoneId
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object StatsAccumUser {
    case class StatsAccum(role: String, device: String, country: String, date: String, lang: String, grade: Int, timeStamp:String, _index:String, _type:String)

    val GS_INPUT_BUCKET = "gs://classting-client-log"
    val GS_OUTPUT_BUCKET = "gs://classting-archive"

    var DATE = "" // very!! very!! very!! important!!
    def get_names(date_str: String) = {
        val year = date_str.substring(0,4)
        val month = date_str.substring(4,6)
        val day = date_str.substring(6,8)
        
        val indexName = "accum-stats-" + year
        // val typeName = "accumulate"
        val typeName = "user"
        val output_path = s"$GS_OUTPUT_BUCKET/$indexName/$typeName/$date_str"
        val del_output_path = output_path.replaceAll(s"/$typeName/","/user-del/")
        (indexName, typeName, output_path, del_output_path)
    }

    
    
    /*** MODIFIED ***/
    def analysisLog(todayDir:String, sc:org.apache.spark.SparkContext, spark:org.apache.spark.sql.SparkSession) {
        import spark.implicits._
        val cal = Calendar.getInstance
        val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    
        cal.setTime(dateFormat.parse(todayDir))
        val timeStamp = cal.getTime
        
        val (indexName, typeName, output_path, del_output_path) = get_names(todayDir)
        
        /*** MODIFIED ***/
        var s3Url = s"$GS_INPUT_BUCKET/logs_" + todayDir + "/*"
        
        // val tmpDF = sqlContext.read.json(s"$GS_INPUT_BUCKET/logs_" + todayDir + "/" + todayDir + "-00_0_api_a.gz")
        // val rowlogsRDD = sqlContext.read.schema(tmpDF.schema).json(sc.textFile(s3Url))
        val rowlogsRDD = spark.read.json(s3Url)
        
        var apiIdx = -1
        var methodIdx = -1
        var langIdx = -1
        var deviceIdx = -1
        var tagIdx = -1
        var roleIdx = -1
        var countryIdx = -1
        var codeIdx = -1
        var curIdx = 0
        rowlogsRDD.columns.foreach { col =>
            col match    {
                case "api" => apiIdx = curIdx
                case "method" => methodIdx = curIdx
                case "code" => codeIdx = curIdx
                case "language" => langIdx = curIdx
                case "device" => deviceIdx = curIdx
                case "tag" => tagIdx = curIdx
                case "role" => roleIdx = curIdx
                case "country" => countryIdx = curIdx
                case _    =>
            }
            curIdx += 1
        }
    
        val signuplogsRDD = rowlogsRDD.rdd.filter    { x =>
                !x.isNullAt(apiIdx) &&
                ( x.getAs[String](apiIdx) == "https://www.classting.com/api/users" ||
                        x.getAs[String](apiIdx) == "https://oauth.classting.com/v1/oauth2/sign_up" ||
                        x.getAs[String](apiIdx) == "https://oauth.classting.com/api/users" )&&
                x.getAs[String](methodIdx) == "POST" &&
                x.getAs[Any](codeIdx) + "" == "200"
        }
    
        var role = "Null"
        val compactLogsRDD = signuplogsRDD.map { x =>
            if( x.getAs[String](roleIdx) != "" )    {
                role = x.getAs[String](roleIdx)
            }
    
            if( langIdx < 0 )    {
                if( deviceIdx > 0 )    {
                    val device = x.getAs[String](deviceIdx) + ""
                    StatsAccum(role, device.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
                else if( tagIdx > 0 )    {
                    val tag = x.getAs[String](tagIdx) + ""
                    StatsAccum(role, tag.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
                else    {
                    StatsAccum(role, "_null", x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
            }
            else    {
                if( deviceIdx > 0 )    {
                    val device = x.getAs[String](deviceIdx) + ""
                    StatsAccum(role, device.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
                else if( tagIdx > 0 )    {
                    val tag = x.getAs[String](tagIdx) + ""
                    StatsAccum(role, tag.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
                else    {
                    StatsAccum(role, "_null", x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
            }
        }
    
        // val signoutlogsRDD = sqlContext.read.schema(tmpDF.schema).json(sc.textFile(s3Url)).rdd.filter    { x =>
        val signoutlogsRDD = rowlogsRDD.rdd.filter    { x =>
                !x.isNullAt(apiIdx) &&
                x.getAs[String](apiIdx).contains("https://www.classting.com/api/users") &&
                x.getAs[String](methodIdx) == "DELETE" &&
                x.getAs[Any](codeIdx) + "" == "200" &&
                x.getAs[String](apiIdx).split("/").length == 6
        }
        var role2 = "Null"
        val compactLogsRDD2 = signoutlogsRDD.map { x =>
            if( x.getAs[String](roleIdx) != "" )    {
                role2 = x.getAs[String](roleIdx)
            }
            else    {
                role2 = "student"
            }
    
            if( langIdx < 0 )    {
                if( deviceIdx > 0 )    {
                    val device = x.getAs[String](deviceIdx) + ""
                    StatsAccum(role2, device.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
                else if( tagIdx > 0 )    {
                    val tag = x.getAs[String](tagIdx) + ""
                    StatsAccum(role2, tag.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
                else    {
                    StatsAccum(role2, "_null", x.getAs[String](countryIdx),
                    todayDir,"_null", 0, timeStamp.toString, indexName, typeName)
                }
            }
            else    {
                if( deviceIdx > 0 )    {
                    val device = x.getAs[String](deviceIdx) + ""
                    StatsAccum(role2, device.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
                else if( tagIdx > 0 )    {
                    val tag = x.getAs[String](tagIdx) + ""
                    StatsAccum(role2, tag.replace(".event", ""), x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
                else    {
                    StatsAccum(role2, "_null", x.getAs[String](countryIdx),
                    todayDir, x.getAs[String](langIdx), 0, timeStamp.toString, indexName, typeName)
                }
            }
        }
        
        val ds1 = compactLogsRDD.toDS
        val ds2 = compactLogsRDD2.toDS
        
        // user
        ds1.coalesce(1).write.option("compression","none").parquet(output_path)
        // user-del
        ds2.coalesce(1).write.option("compression","none").parquet(del_output_path)
    
    
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
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        val fs = FileSystem.get(new URI("gs://classting-archive"), sc.hadoopConfiguration)

        date_list.foreach{
            todayDir =>
            //e.g. gs://classting-archive/accum-stats-2018/user/20181001
            val (indexName, typeName, output_path, del_output_path) = get_names(todayDir)
        
            fs.delete(new Path(output_path), true) // isRecusrive= true
            println(s"delete... $output_path")
        
            fs.delete(new Path(del_output_path), true) // isRecusrive= true
            println(s"delete... $del_output_path")
        
            analysisLog(todayDir, sc, spark )
        }
    }

}

