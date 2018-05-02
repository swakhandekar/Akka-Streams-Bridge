package models

case class Message(txId: String, tableName: String, payload: Array[Byte]) {}
