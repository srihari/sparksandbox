package com.sparksandbox

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class PurchasesDF(purchasesDF: DataFrame, session: SparkSession) {

  import session.implicits._

  val purchasesFrame: Dataset[PurchaseTransaction] = purchasesDF.flatMap(row => {
    if ("household_key".equals(row.getString(0))) {
      None
    } else {
      val householdKey = row.getString(0)
      val basketId = row.getString(1)
      Some(PurchaseTransaction(householdKey, basketId, "", "", "", "", "", "", "", "", "", ""))
    }
  })

  def basketsPerHousehold(): collection.Map[String, Long] = {

    val fingerprintedTxns: Dataset[(Long, PurchaseTransaction)] = purchasesFrame.map((purchase: PurchaseTransaction) => {
      (purchase.fingerprint(), purchase)
    })
    val distinctFingerprints: Dataset[(Long, PurchaseTransaction)] = fingerprintedTxns.distinct()
    val distinctTxns = distinctFingerprints.groupBy("_1")


    Map[String, Long]()
  }

}
