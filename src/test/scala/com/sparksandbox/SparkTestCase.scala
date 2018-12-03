package com.sparksandbox


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SparkTestCase extends FunSuite with BeforeAndAfterEach {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(getClass.getCanonicalName)
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val sparkContext: SparkContext = sparkSession.sparkContext

  override protected def beforeEach(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

}
