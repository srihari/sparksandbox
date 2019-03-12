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

  def household360(): RDD[String] = {
    distinctHouseholds
  }

  private def distinctHouseholds = {
    val mappedRDD = purchasesRDD.map((txn: PurchaseTransaction) => (txn.householdKey, txn.householdKey))
    val reducedRDD = mappedRDD.reduceByKey((accumulator, householdKey) => householdKey)
    reducedRDD.values.map((householdKey: String) => householdKey)
  }

  def basketsPerHouseHold(): collection.Map[String, Long] = {
    val fingerprintedRDD: RDD[(Long, PurchaseTransaction)] = purchasesRDD.map((txn: PurchaseTransaction) => (txn.fingerprint(), txn))
    val reducedRDD: RDD[(Long, PurchaseTransaction)] = fingerprintedRDD.reduceByKey((accumulator, txn) => txn)
    val hhBasketPairs = reducedRDD.values.map((txn: PurchaseTransaction) => (txn.householdKey, txn.basketId))
    val basketsPerHousehold = hhBasketPairs.countByKey()
    basketsPerHousehold
  }


}

