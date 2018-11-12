package com.classting

/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession // we're going to use spark2.0

// Timer
import org.joda.time.Duration
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTime
// email
import org.apache.commons.mail._
import java.net._

object SimpleApp {

	def getElapsedSeconds(start: DateTime, end: DateTime): Int = {
		val elapsed_time = new Duration(start.getMillis(), end.getMillis())
		val elapsed_seconds = elapsed_time.toStandardSeconds().getSeconds()
		(elapsed_seconds)
	}
	def sendEmail(title:String, msg: String, to: String) = {
		val localhost = InetAddress.getLocalHost.toString
		val name = localhost.split("/")(0)
		val ip = localhost.split("/")(1)
		val email = new SimpleEmail();
		email.setHostName(ip);
		email.setFrom(s"noreply@${name}"); //FROM
		email.setSubject(title); //SUBJECT
		email.setMsg(msg); //Email Message 
		email.addTo(to); //TO
		email.send()
	}
    def main(args: Array[String]) {
		val usage = s"""
		Usage: [DATE] [Email]
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
		println(s"""
		----------------------------
		DATE: ${args(0)}
		Email: ${args(1)}
		----------------------------
		""")
		val DATE = args(0)
		val EMAIL_TO = args(1)

		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._

        // this starts the clock
        val start_time = DateTime.now()

		// email content
		val emalTitle = s"test for args: ${DATE}, ${EMAIL_TO}"
		var emailContent = ""

		// this stops the clock
		val end_time = DateTime.now()
		val elapsed_secs = getElapsedSeconds(start_time, end_time)
		val min = elapsed_secs/60
		val sec = elapsed_secs%60
		// print out elapsed time
		emailContent += f"Elapsed time: ${min}%d minutes ${sec}%d seconds"
		emailContent += "\n-------------------------\n"
		emailContent += spark.conf.getAll.mkString("\n")

		sendEmail(emalTitle, emailContent, EMAIL_TO)

    }

}

