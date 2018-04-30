import java.nio.ByteBuffer

import com.lightbend.kafka.scala.streams.{KStreamS, KTableS}
import com.sksamuel.avro4s.AvroInputStream
import models.GenericWrapper
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization.parsingFingerprint64
import org.apache.kafka.streams.kstream.Serialized

class Consumer {
  private val DELIMITER = '^'

  private def readSchema(): KStreamS[String, String] = {
    import serde.ConsumerSerde.readSchemaConsumed
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

  private def readMessage(): KStreamS[String, Array[Byte]] = {
    import serde.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, Array[Byte]]("ogg-payload")
  }


  private def processPayload(messageStream: KStreamS[String, Array[Byte]]): KStreamS[Long, GenericWrapper] = {
    messageStream.flatMap((_: String, message: Array[Byte]) => {
      var messages: List[(Long, GenericWrapper)] = List()
      val buffer = ByteBuffer.allocate(1024)

      message.foreach(byte => {
        byte.toChar match {
          case DELIMITER => {
            val input = AvroInputStream.binary[GenericWrapper](buffer.array())
            if (input.iterator.hasNext) {
              val genericWrapper: GenericWrapper = input.iterator.next()
              messages = (genericWrapper.schema_fingerprint, genericWrapper) :: messages
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
    import serde.ConsumerSerde.{joinSchemaPayloadSerde, _}
    val schemaTable: KTableS[Long, String] = streamToTable[Long, String](transformSchemaStream())
    val messageStream: KStreamS[Long, GenericWrapper] = processPayload(readMessage())

    messageStream.join(
      schemaTable,
      (genericWrapper: GenericWrapper, schema: String) => s"${genericWrapper.table_name} => ${schema}"
    )
      .peek((fingerprint, value) => println(s"${fingerprint} =>> ${value}"))
  }
}
