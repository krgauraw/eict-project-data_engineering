package com.cloudxlab.logparser.impl

import com.cloudxlab.logparser.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class LogParser extends Serializable {

	val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

	def parseLogEntry(entry: String):
	LogEntry = {
		val result = PATTERN.findFirstMatchIn(entry)
		if (null != result && !result.isEmpty) {
			val data = result.get
			LogEntry(data.group(1), data.group(4), data.group(6), data.group(8).toInt)
		} else LogEntry("", "", "", 0)

	}

	def getUniqueHttpCode(log: RDD[LogEntry]): Array[(Int, Int)] = log.map(x => (x.respCode, 1)).reduceByKey(_ + _).collect()

	def getTopNUrls(log: RDD[LogEntry], count: Int): Array[(String, Int)] = log.map(x => (x.url, 1)).reduceByKey(_ + _).takeOrdered(count)(Ordering[Int].reverse.on(x => x._2))

	def getTopNTrafficData(log: RDD[LogEntry], count: Int): Map[String, Array[(String, Int)]] = {
		val temp = log.map(x => (x.timeStamp.substring(0, 14), 1)).reduceByKey(_ + _)
		val leastTrafficData = temp.takeOrdered(count)(Ordering[Int].on(x => x._2))
		val peakTrafficData = temp.takeOrdered(count)(Ordering[Int].reverse.on(x => x._2))
		Map("minTraffic" -> leastTrafficData, "maxTraffic" -> peakTrafficData)
	}
}

case class LogEntry(host: String = "", timeStamp: String = "", url: String = "", respCode: Int = 0)


object DriverProgram {

	val usage =
		"""
        Usage: DriverProgram <file_or_directory_in_hdfs>
        Example: DriverProgram /data/spark/project/NASA_access_log_Aug95.gz
    """

	def main(args: Array[String]): Unit = {

		if (args.length != 2) {
			println("Expected:2 , Provided: " + args.length)
			println(usage)
			return;
		}
		val parser = new LogParser()
		val logFilePath = args(1)
		val sc = SparkUtil.getSparkContext("logParser")
		val logFile = sc.textFile(logFilePath)
		// logs rdd in the form of case class
		val logsRdd: RDD[LogEntry] = logFile.map(parser.parseLogEntry).filter(!_.respCode.equals(0)).persist(StorageLevel.MEMORY_AND_DISK)

		// Problem 1 : Write spark code to find out top 10 requested URLs along with a count of the number of times they have been requested.
		val urls = parser.getTopNUrls(logsRdd, 10)
		println("Top 10 Urls Are As Below : ")
		println("---------------------------------")
		for (url <- urls) {
			println("Url: " + url._1 + ", Count:" + url._2)
		}
		println("---------------------------------")


		val trafficData = parser.getTopNTrafficData(logsRdd, 5)

		// Problem 2 : Write spark code to find out the top five-time frame for high traffic (which day of the week or hour of the day receives
		//             peak traffic
		val peakTrafficDays: Array[(String, Int)] = trafficData.getOrElse("maxTraffic", Array[(String, Int)]())
		println("Top 5 Timeframes For High Traffic:")
		println("---------------------------------")
		for (pt <- peakTrafficDays) {
			println("Timestamp: " + pt._1 + ", Count:" + pt._2)
		}
		println("---------------------------------")


		// Problem 3 : Write spark code to find out top 5 time frames of least traffic (which day of the week or hour of the day receives
		//             least traffic
		val leastTrafficDays: Array[(String, Int)] = trafficData.getOrElse("minTraffic", Array[(String, Int)]())
		println("Top 5 Timeframes For Least Traffic:")
		println("---------------------------------")
		for (lt <- leastTrafficDays) {
			println("Timestamp: " + lt._1 + ", Count:" + lt._2)
		}
		println("---------------------------------")


		//Problem 4 : Write Spark code to find out unique HTTP codes returned by the server along with count
		val uniqueCodes = parser.getUniqueHttpCode(logsRdd)
		println("Unique Http Codes Are As Below : ")
		println("---------------------------------")
		for (uc <- uniqueCodes) {
			println("Http Code: " + uc._1 + ", Count: " + uc._2)
		}
		println("---------------------------------")

	}
}
