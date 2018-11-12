package com.classting

/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0

object SimpleApp {

    def main(args: Array[String]) {
        val usage = s"""
        Usage: [START_DATE] [END_DATE]
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
        val END_DATE = args(1)

        println(s"""
        ----------------------------
        START_DATE: ${START_DATE}
        END_DATE: ${END_DATE}
        ----------------------------
        """)

        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        spark.conf.getAll.foreach{
            case(key, value) =>
            println("=" * 40)
            println(key)
            println(value)
        }

    }

}

