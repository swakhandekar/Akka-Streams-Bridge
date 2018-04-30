package models

case class DBChange(txId: String, tableName: String, payload: Array[Byte]) {}
