package com.cloudxlab.logparser.impl

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

/**
 * Test Cases For LogParser
 *
 */
class LogParserTest extends FlatSpec with SharedSparkContext {

	val parser: LogParser = new LogParser()

	def getInputRDD(): RDD[LogEntry] = {
		val inputRecords = Array(
			LogEntry("haraway.ucet.ufl.edu", "01/Aug/1995:00:04:30 -0400", "/images/MOSAIC-logosmall.gif", 200),
			LogEntry("piweba3y.prodigy.com", "01/Aug/1995:00:04:37 -0400", "/images/NASA-logosmall.gif", 200),
			LogEntry("198.248.59.123", "01/Aug/1995:01:28:57 -0400", "/images/NASA-logosmall.gif", 400))
		sc.parallelize(inputRecords)
	}

	"parseLogEntry with valid input" should "return a LogEntry object having all data" in {
		val input = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
		val result: LogEntry = parser.parseLogEntry(input)
		assert(null != result)
		assert(result.host.equalsIgnoreCase("in24.inetnebr.com"))
		assert(result.timeStamp.equalsIgnoreCase("01/Aug/1995:00:00:01 -0400"))
		assert(result.url.equalsIgnoreCase("/shuttle/missions/sts-68/news/sts-68-mcc-05.txt"))
		assert(result.respCode == 200)
	}

	"parseLogEntry with invalid input" should "return a empty LogEntry object" in {
		val input = "128.159.117.22 - - [30/Aug/1995:13:45:35 -0400] \"���.�\u000B2�.�>� .�\u00032����������.�\u0006� \" 400 -"
		val result: LogEntry = parser.parseLogEntry(input)
		assert(null != result)
		assert(result.host.equalsIgnoreCase(""))
		assert(result.timeStamp.equalsIgnoreCase(""))
		assert(result.url.equalsIgnoreCase(""))
		assert(result.respCode == 0)
	}

	"getUniqueHttpCode with valid input" should "return unique host and its respective count" in {
		val result: Array[(Int, Int)] = parser.getUniqueHttpCode(getInputRDD())
		assert(null != result && !result.isEmpty)
		assert(result.size == 2)
		val resultMap = result.toMap
		assert(null != resultMap && resultMap.size == 2)
		assert(resultMap.getOrElse(200, 0) == 2)
		assert(resultMap.getOrElse(400, 0) == 1)
	}

	"getTopNUrls with valid input for given count" should "return n urls having maximum hit with corresponding count value" in {
		val result: Array[(String, Int)] = parser.getTopNUrls(getInputRDD(), 2)
		assert(null != result && !result.isEmpty)
		assert(result.size == 2)
		val resultMap = result.toMap
		assert(null != resultMap && resultMap.size == 2)
		assert(resultMap.getOrElse("/images/MOSAIC-logosmall.gif", 0) == 1)
		assert(resultMap.getOrElse("/images/NASA-logosmall.gif", 0) == 2)
	}

	"getTopNTrafficData with valid input for given count" should "return n timestamp having maximum hit with corresponding count value" in {
		val result: Map[String, Array[(String, Int)]] = parser.getTopNTrafficData(getInputRDD(), 2)
		assert(null != result && !result.isEmpty)
		assert(result.size == 2)
		val maxTraffic = result.getOrElse("maxTraffic", Array[(String, Int)]())
		assert(maxTraffic.size == 2)
		val maxTrafficMap = maxTraffic.toMap
		assert(maxTrafficMap.getOrElse("01/Aug/1995:00", 0) == 2)
		val minTraffic = result.getOrElse("minTraffic", Array[(String, Int)]())
		assert(minTraffic.size == 2)
		val minTrafficMap = minTraffic.toMap
		assert(minTrafficMap.getOrElse("01/Aug/1995:01", 0) == 1)
	}


}
