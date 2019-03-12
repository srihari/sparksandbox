package com.sparksandbox

import org.apache.spark.rdd.RDD

class PurchasesRDD(source: RDD[String]) {


  val purchasesRDD: RDD[PurchaseTransaction] =
    source.flatMap((row: String) => {
      val columns = row.split(",")
      if (!"household_key".equals(columns.head)) {
        val householdKey = columns(0)
        val basketId = columns(1)
        val day = columns(2)
        val productId = columns(3)
        val quantity = columns(4)
        val salesValue = columns(5)
        val storeId = columns(6)
        val retailDiscount = columns(7)
        val txnTime = columns(8)
        val weekNo = columns(9)
        val couponDiscount = columns(10)
        val couponMatchDiscount = columns(11)
        Some(PurchaseTransaction(householdKey, basketId, day, productId, quantity,
          salesValue, storeId, retailDiscount, txnTime,
          weekNo, couponDiscount, couponMatchDiscount))
      }
      else None
    })

  def household360(): RDD[String] = {
    val pairRDD = purchasesRDD.map((txn: PurchaseTransaction) => (txn.householdKey, txn))
    val groupedRDD = pairRDD.groupByKey()
    groupedRDD.map(tuple => {
      val key = tuple._1
      val baskets = tuple._2
      val numberOfPurchases = baskets.size
      var lifetimeSpend = 0.0
      baskets.foreach({
        lifetimeSpend += _.netSalesValue
      })

      StringBuilder.newBuilder.++=(key).append(',')
        .append(numberOfPurchases).append(',')
        .append(lifetimeSpend).append(',')
        .toString()

    })
  }

  def distinctHouseholds: RDD[String] = {
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

