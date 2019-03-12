package com.sparksandbox

import java.security.MessageDigest


case class PurchaseTransaction(_householdKey: String, _basketId: String, _day: String,
                               _productId: String, _quantity: String, _salesValue: String, _storeId: String,
                               _retailDiscount: String, _txnTime: String,
                               _weekNo: String, _couponDiscount: String, _couponMatchDiscount: String) extends Serializable {
  def householdKey: String = _householdKey

  def basketId: String = _basketId

  def netSalesValue: Double = _salesValue.toDouble

  def fingerprint(): Long = {
    val concatenatedString = StringBuilder.newBuilder.++=(householdKey).++=(basketId).toString()
    val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
    algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
    BigInt(algorithm.digest()).longValue()
  }

}
