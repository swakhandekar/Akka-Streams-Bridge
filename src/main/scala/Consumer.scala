import java.nio.ByteBuffer
import java.util.UUID.randomUUID

import com.lightbend.kafka.scala.streams.{KStreamS, KTableS}
import com.sksamuel.avro4s.AvroInputStream
import models.{DBChange, GenericWrapper}
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization.parsingFingerprint64
import org.apache.kafka.streams.kstream.Serialized

class Consumer {
  private val DELIMITER = '^'

  private def readSchema(): KStreamS[String, String] = {
    import serializers.ConsumerSerde.readSchemaConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, String]("ogg-schema")
  }

  private def transformSchemaStream(): KStreamS[Long, String] = {
    val schemaStream: KStreamS[String, String] = readSchema()
    schemaStream.map((_, schema: String) => {
      val schemaFingerprint = parsingFingerprint64(new Schema.Parser().parse(schema))
      (schemaFingerprint, schema)
    })
  }

  private def streamToTable[K, V](stream: KStreamS[K, V])(implicit serialized: Serialized[K, V]): KTableS[K, V] = {
    stream.groupByKey.reduce((_, value2: V) => value2)
  }

  private def readTxMessage(): KStreamS[String, Array[Byte]] = {
    import serializers.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, Array[Byte]]("ogg-payload")
  }


  private def processTxMessage(messageStream: KStreamS[String, Array[Byte]]): KStreamS[Long, DBChange] = {
    messageStream.flatMap((_: String, message: Array[Byte]) => {
      val txId: String = randomUUID().toString
      var messages: List[(Long, DBChange)] = List()
      val buffer = ByteBuffer.allocate(1024)

      message.foreach(byte => {
        byte.toChar match {
          case DELIMITER => {
            val input = AvroInputStream.binary[GenericWrapper](buffer.array())
            if (input.iterator.hasNext) {
              val genericWrapper: GenericWrapper = input.iterator.next()
              val dbChange = DBChange(txId, genericWrapper.table_name, genericWrapper.payload)
              messages = (genericWrapper.schema_fingerprint, dbChange) :: messages
            }
            buffer.clear()
          }
          case _ => buffer.put(byte)
        }
      })
      messages
    })
  }

  def joinSchemaPayload(): Unit = {
    import serializers.ConsumerSerde.{joinSchemaDBChange, serializedLongString}
    val schemaTable: KTableS[Long, String] = streamToTable[Long, String](transformSchemaStream())
    val messageStream: KStreamS[Long, DBChange] = processTxMessage(readTxMessage())

    messageStream.join(
      schemaTable,
      (dbChange: DBChange, schema: String) => s"${dbChange.tableName} => ${schema}"
    )
      .peek((fingerprint, value) => println(s"${fingerprint} =>> ${value}"))
  }
}
