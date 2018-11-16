package com.classting

/* StatsAccumClass.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext

import java.time.ZoneId
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object StatsAccumClass {
    case class StatsAccum(role: String, device: String, country: String, date: String, lang: String, grade: Int, timeStamp:String, _index:String, _type:String)


    val GS_INPUT_BUCKET = "gs://classting-client-log"
    val GS_OUTPUT_BUCKET = "gs://classting-archive"
    val obj = "user"
    var DATE = "" // very!! very!! very!! important!!
    def get_names(date_str: String) = {
        val year = date_str.substring(0,4)
        val month = date_str.substring(4,6)
        val day = date_str.substring(6,8)

        val indexName = "accum-stats-" + year
        val typeName = "class"
        val output_path = s"$GS_OUTPUT_BUCKET/$indexName/$typeName/$date_str"
        (indexName, typeName, output_path)
    }
    val cal = Calendar.getInstance
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")

    /*** MODIFIED ***/
    def analysisLog(output_path: String, indexName:String, typeName:String, todayDir:String, _isPrd:Int, timeStamp: String,
        sc:org.apache.spark.SparkContext, sqlContext:org.apache.spark.sql.SQLContext, spark:org.apache.spark.sql.SparkSession ) {
        import spark.implicits._

        val isPrd = _isPrd
        
        /*** MODIFIED ***/
        var s3Url = s"$GS_INPUT_BUCKET/logs_" + todayDir + "/" + todayDir + "-10_*.gz"
        if (isPrd > 0) {
            s3Url = s"$GS_INPUT_BUCKET/logs_" + todayDir + "/*"
        }

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
        rowlogsRDD.columns.map    { col =>
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

        val makelogsRDD = rowlogsRDD.rdd.filter    { x =>
                !x.isNullAt(apiIdx) &&
                x.getAs[String](apiIdx) == "https://www.classting.com/api/classes" &&
                x.getAs[String](methodIdx) == "POST" &&
                x.getAs[Any](codeIdx) + "" == "200"
        }

        var role = "Null"
        val compactLogsRDD = makelogsRDD.map { x =>
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

        val dellogsRDD = rowlogsRDD.rdd.filter    { x =>
                !x.isNullAt(apiIdx) &&
                x.getAs[String](apiIdx).contains("https://www.classting.com/api/classes") &&
                x.getAs[String](methodIdx) == "DELETE" &&
                x.getAs[Any](codeIdx) + "" == "200" &&
                x.getAs[String](apiIdx).split("/").length == 6
        }

        var role2 = "Null"
        val compactLogsRDD2 = dellogsRDD.map { x =>
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
        
        val del_output_path = output_path.replaceAll(s"/$typeName/","/class-del/")
        
        if( isPrd > 8 ) {
            // compactLogsRDD.coalesce(partSize/5, false).saveToEs(indexName + "/" + typeName, Map("es.nodes" -> esNode))
            ds1.coalesce(1).write.option("compression","none").parquet(output_path)
            // compactLogsRDD2.coalesce(partSize/5, false).saveToEs(indexName + "/" + "class-del", Map("es.nodes" -> esNode))
            ds2.coalesce(1).write.option("compression","none").parquet(del_output_path)
        }
        else if( isPrd == 5 )    {
            // compactLogsRDD.coalesce(partSize/5, false).saveToEs(indexName + "/" + typeName, Map("es.nodes" -> esNode))
            ds1.coalesce(1).write.option("compression","none").parquet(output_path)
            // compactLogsRDD2.coalesce(partSize/5, false).saveToEs(indexName + "/" + "class-del", Map("es.nodes" -> esNode))
            ds2.coalesce(1).write.option("compression","none").parquet(del_output_path)
        }

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
            DATE = start.minusDays( i ).format(tmp_dateFormat)
            // guess as a kind of java bug 
            if(DATE.endsWith("1231")){
                val year = DATE.substring(0,4).toInt
                DATE = DATE.replaceAll(year.toString, (year - 1).toString)
            }
            DATE
        }

        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        val fs = FileSystem.get(new URI("gs://classting-archive"), sc.hadoopConfiguration)
        date_list.foreach{
            todayDir =>
            cal.setTime(dateFormat.parse(todayDir))
            val timeStamp = cal.getTime.toString
            val (indexName, typeName, output_path) = get_names(todayDir)
            println(output_path)
            fs.delete(new Path(output_path), true) // isRecusrive= true
            println(s"delete... $output_path")

            val del = output_path.replaceAll(s"/$typeName/","/class-del/")

            println(del)
            fs.delete(new Path(del), true) // isRecusrive= true
            println(s"delete... $del")

            analysisLog(output_path, indexName, typeName, todayDir, isPrd, timeStamp, sc, sqlContext, spark)
        }
    }

}

