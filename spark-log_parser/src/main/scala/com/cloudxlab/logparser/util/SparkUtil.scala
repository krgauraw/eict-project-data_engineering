package com.cloudxlab.logparser.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {

	def getSparkContext(appName: String): SparkContext = {
		if(null==appName)
			return null
		val conf = new SparkConf().setAppName(appName).setMaster("local")
		val sc = new SparkContext(conf);
		sc.setLogLevel("WARN")
		sc
	}
}
