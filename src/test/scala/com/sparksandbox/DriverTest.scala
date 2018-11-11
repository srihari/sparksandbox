package com.sparksandbox

import org.apache.spark.rdd.RDD

class DriverTest extends SparkTestCase {

  test("Initate a Spark Session") {
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val intRDD: RDD[Int] = sparkContext.parallelize(data, 2)

    //    val evenRDD = intRDD.filter((number: Int) => {
    //      number % 2 == 0
    //    })
    val squaresRDD: RDD[Int] = intRDD.map((number: Int) => {
      number * number
    })

    val sumOfSquares = squaresRDD.reduce((sumOfSquares, number) => sumOfSquares + number)
    print("Sum of Squares = " + sumOfSquares)

  }
}
