package com.sparksandbox


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.log4j.{Level, Logger}

class SparkTestCase extends FunSuite with BeforeAndAfterEach {
  var sparkContext: SparkContext = _

  override protected def beforeEach(): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName(getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    sparkContext = session.sparkContext
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

}
