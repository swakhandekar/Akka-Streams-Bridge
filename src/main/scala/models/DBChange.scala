package models

case class DBChange(tableName: String, schema: String, payload: Array[Byte])
