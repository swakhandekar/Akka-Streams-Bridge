package models

case class DBChange(txId: String, tableName: String, schema: String, payload: Array[Byte]) {}
