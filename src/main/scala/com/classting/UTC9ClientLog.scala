package com.classting

/* UTC9ClientLog.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0
import org.apache.spark.sql.SQLContext

import java.time.ZoneId
import java.time.LocalDate

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.functions._
object UTC9ClientLog {

    val INPUT_BUCKET = "s3://classting-client-log"
    val OUTPUT_BUCKET = "s3://classting-archive"

    val input_dir = s"${INPUT_BUCKET}/logs"
    val output_dir = s"${OUTPUT_BUCKET}/utc9/classting-client-log/logs"

    def run(spark:org.apache.spark.sql.SparkSession, df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], target_date: String, output_path: String) = {
        import spark.implicits._
        import org.apache.spark.sql.types.StringType
        import org.apache.spark.sql.types.LongType
        df.
        withColumn("grade", when($"grade".isNull,lit(-999)).otherwise($"grade".cast(LongType))).
        withColumn("code", when($"code".isNull,lit("_null")).otherwise($"code".cast(StringType))).
        withColumn("date_str", lit(target_date)).
        withColumn("kr_date", to_date($"date_str")).
        drop($"date_str").
        repartition(4).
        write.mode("overwrite").option("compression","gzip").parquet(output_path)
        output_path
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

        val start = LocalDate.of(_year, _month, _day)
        val date_list = (0 to n-1).map{
            i =>
            val previous_date = start.minusDays( i+1 ).toString
            val target_date = start.minusDays( i ).toString
            (previous_date, target_date)
        }.toArray

        val spark = SparkSession.builder()
        .config("spark.ui.showConsoleProgress", false)
        .getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        /*run period*/
        date_list.foreach{
          case (previous_date, target_date) =>
            val input_list = List(s"${input_dir}_${previous_date.replaceAll("-", "")}", s"${input_dir}_${target_date.replaceAll("-", "")}")
            val df = spark.read.json(input_list:_* ).where(s"ftime between '${previous_date}T15' and '${target_date}T15'")

            val day = target_date.replaceAll("-","")
            val output_path = s"${output_dir}_$day"
            println(input_list,output_path)

            run(spark, df, target_date, output_path)
        }
    }
}

