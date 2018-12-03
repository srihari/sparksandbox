package com.sparksandbox


class FramesBasedDriverTest extends SparkTestCase {

  test("Get Baskets Per Household") {
    val start = System.currentTimeMillis()
    val purchasesDS = sparkSession.read.option("header", "true").csv("data/transactions.csv")

    println(purchasesDS)

    val end = System.currentTimeMillis()
    println("Execution took " + (end - start))
  }

}
