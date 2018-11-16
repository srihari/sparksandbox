package com.sparksandbox

import java.security.MessageDigest

class PurchaseTransaction(_householdKey: String, _basketId: String) extends Serializable {
  def householdKey = _householdKey

  def basketId = _basketId

  def fingerprint(): Long = {
    val concatenatedString = StringBuilder.newBuilder.++=(householdKey).++=(basketId).toString()
    val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
    algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
    BigInt(algorithm.digest()).longValue()
  }

}
