package com.sparksandbox

import org.apache.spark.rdd.RDD

import scala.collection.mutable

class TransactionsRDD(source: RDD[String]) {
  val purchasesRDD: RDD[PurchaseTransaction] =
    source.flatMap((row: String) => {
      val columns = row.split(",")
      if (!"household_key".equals(columns.head)) {
        val householdKey = columns(0)
        val basketId = columns(1)
        Some(new PurchaseTransaction(householdKey, basketId))
      }
      else None
    })


  def basketsPerHouseHold(): mutable.Map[String, Int] = {
    val basketsPerHousehold: mutable.Map[String, Int] = mutable.Map()


    basketsPerHousehold
  }


}

