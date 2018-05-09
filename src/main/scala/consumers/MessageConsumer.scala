package consumers

import java.nio.ByteBuffer
import java.util.UUID.randomUUID

import com.lightbend.kafka.scala.streams.KStreamS
import com.sksamuel.avro4s.AvroInputStream
import models.{DBChange, Event, GenericWrapper}

import scala.collection.mutable

class MessageConsumer(schemaMap: mutable.Map[Long, String]) {
  private val DELIMITER = '^'

  private def readTxMessage(): KStreamS[String, Array[Byte]] = {
    import serializers.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, Array[Byte]]("ogg-payload")
  }


  private def processTxMessage(messageStream: KStreamS[String, Array[Byte]]): KStreamS[String, Event] = {
    messageStream.map((_: String, message: Array[Byte]) => {
      val txId: String = randomUUID().toString
      var dbChanges: mutable.MutableList[DBChange] = mutable.MutableList()
      val buffer = ByteBuffer.allocate(1024)

      message.foreach(byte => {
        byte.toChar match {
          case DELIMITER =>
            val input = AvroInputStream.binary[GenericWrapper](buffer.array())
            if (input.iterator.hasNext) {
              val genericWrapper: GenericWrapper = input.iterator.next()
              val schemaFingerprint = genericWrapper.schema_fingerprint
              val schema = schemaMap(schemaFingerprint)
              dbChanges += DBChange(genericWrapper.table_name, schema, genericWrapper.payload)
            }
            buffer.clear()
          case _ => buffer.put(byte)
        }
      })
      (txId, Event(dbChanges.toList))
    })
  }

  def start(): Unit = {
    val messageStream = readTxMessage()
    processTxMessage(messageStream)
      .foreach((_, event) => {
        println(event)
      })
  }
}
