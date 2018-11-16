package com.sparksandbox

import org.apache.spark.rdd.RDD

class PurchasesRDD(source: RDD[String]) {

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


  def basketsPerHouseHold(): collection.Map[String, Long] = {
    val fingerprintedRDD: RDD[(Long, PurchaseTransaction)] = purchasesRDD.map((txn: PurchaseTransaction) => (txn.fingerprint(), txn))
    val reducedRDD: RDD[(Long, PurchaseTransaction)] = fingerprintedRDD.reduceByKey((accumulator, txn) => txn)
    val hhBasketPairs = reducedRDD.values.map((txn:PurchaseTransaction) => (txn.householdKey, txn.basketId))
    val basketsPerHousehold = hhBasketPairs.countByKey()
    basketsPerHousehold
  }



}

