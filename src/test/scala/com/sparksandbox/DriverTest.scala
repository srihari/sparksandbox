package com.sparksandbox

import org.apache.spark.rdd.RDD

class DriverTest extends SparkTestCase {

  test("Initate a Spark Session") {

    val intRDD: RDD[Int] = sparkContext.parallelize(1 to 100, 10)

    val evenRDD = intRDD.filter((number: Int) => {
      number % 2 == 0
    })

    val squaresRDD: RDD[Int] = intRDD.map((number: Int) => {
      number * number
    })

    val sumOfSquares = squaresRDD.reduce((sumOfSquares, number) => sumOfSquares + number)
    println("Sum of Squares = " + sumOfSquares)


    val sumOfEvens = evenRDD.reduce((sumOfEvens, number) => sumOfEvens + number)
    println("Sum of Evens " + sumOfEvens)


    val seqOp = (zeroValue: Int, number : Int) => {zeroValue + number}
    val combOp = (partSum: Int, finalSum: Int) => partSum + finalSum

    val aggrSum = intRDD.aggregate(0)(seqOp,combOp)
    println(aggrSum)

  }

  test("Process a large file") {
    val start = System.currentTimeMillis()
    val transactionsRDD = sparkContext.textFile("data/transactions.csv", 5)
    val numberOfTxns = transactionsRDD.count()
    val end = System.currentTimeMillis()
    println("Execution took " + (end - start))
  }

  test("Get Baskets Per Household") {
    val start = System.currentTimeMillis()
    val transactions = new TransactionsRDD(sparkContext.textFile("data/transactions.csv", 5))
    val basketsPerHousehold = transactions.basketsPerHouseHold()

    assert(basketsPerHousehold.nonEmpty)

    val end = System.currentTimeMillis()
    println("Execution took " + (end - start))

  }
}
