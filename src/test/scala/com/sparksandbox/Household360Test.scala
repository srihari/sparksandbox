package com.sparksandbox

class Household360Test extends SparkTestCase {


  test("Process a large file") {
    val start = System.currentTimeMillis()
    val transactionsRDD = sparkContext.textFile("data/transactions.csv", 5)
    val numberOfTxns = transactionsRDD.count()
    val end = System.currentTimeMillis()
    println("Execution took " + (end - start))
  }

  test("Get Baskets Per Household") {
    val start = System.currentTimeMillis()
    val transactions = new PurchasesRDD(sparkContext.textFile("data/transactions.csv", 5))
    val basketsPerHousehold = transactions.basketsPerHouseHold()
    assert(basketsPerHousehold.nonEmpty)
    println(basketsPerHousehold)

    val end = System.currentTimeMillis()
    println("Execution took " + (end - start))

  }

  test("Build Household 360"){
    val transactions = new PurchasesRDD(sparkContext.textFile("data/transactions.csv"))
    val distinctHouseholds = transactions.household360()
    distinctHouseholds.saveAsTextFile("data/output/household360")
  }

}
