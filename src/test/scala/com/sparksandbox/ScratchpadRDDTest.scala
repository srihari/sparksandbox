package com.sparksandbox

import org.apache.spark.rdd.RDD

class ScratchpadRDDTest extends SparkTestCase {


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


    val seqOp = (zeroValue: Int, number: Int) => {
      zeroValue + number
    }
    val combOp = (partSum: Int, finalSum: Int) => partSum + finalSum

    val aggrSum = intRDD.aggregate(0)(seqOp, combOp)
    println(aggrSum)

  }

  test("Simple spark code with rdds"){

  }
}
