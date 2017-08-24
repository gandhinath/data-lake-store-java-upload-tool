package com.starbucks.analytics.eventhub

class Event(val uri: String, val sharedAccessSignatureToken: String) {
  def toJson: String =
    s"""{
       |    "uri": "$uri",
       |    "sharedAccessSignatureToken": "$sharedAccessSignatureToken"
       |}
     """.stripMargin
}
