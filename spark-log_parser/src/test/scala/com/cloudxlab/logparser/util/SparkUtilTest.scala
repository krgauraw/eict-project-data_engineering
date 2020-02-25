package com.cloudxlab.logparser.util

import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers.convertToAnyShouldWrapper

/**
 * Test Cases For SparkUtil
 */
class SparkUtilTest extends FlatSpec {

	"getSparkContext with valid app name" should "return spark context" in {
		val context: SparkContext = SparkUtil.getSparkContext("local")
		context != null shouldEqual true
		context.isStopped shouldEqual false
	}

	"getSparkContext with invalid app name" should "return null" in {
		val context: SparkContext = SparkUtil.getSparkContext(null)
		context shouldEqual null
	}
}
