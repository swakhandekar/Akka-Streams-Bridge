import com.lightbend.kafka.scala.streams.{KStreamS, KTableS}
import org.apache.avro.SchemaNormalization.fingerprint64
import org.apache.kafka.streams.kstream.Serialized

class Consumer {
  private def readSchema(): KStreamS[String, String] = {
    import serde.ConsumerSerde.readSchemaConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, String]("ogg-schema")
  }

  private def transformSchemaStream(): KStreamS[Long, String] = {
    val schemaStream: KStreamS[String, String] = readSchema()
    schemaStream.map((_, schema) => {
      val schemaFingerprint = fingerprint64(schema.getBytes)
      (schemaFingerprint, schema)
    })
  }

  private def streamToTable[K, V](stream: KStreamS[K, V])(implicit serialized: Serialized[K, V]): KTableS[K, V] = {
    stream.groupByKey.reduce((_, value2: V) => value2)
  }

  private def readPayload(): KStreamS[Long, String] = {
    import serde.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, String]("ogg-payload").map((fingerprint, payload) => (fingerprint.toLong, payload))
  }

  def joinSchemaPayload(): Unit = {
    import serde.ConsumerSerde.{joinSchemaPayloadSerde, serializedLongString}
    val schemaTable = streamToTable(transformSchemaStream())
    val payloadStream: KStreamS[Long, String] = readPayload()

    payloadStream.join(
      schemaTable, (payload: String, schema: String) => s"${schema} ==> ${payload}"
    ).peek((fingerprint, value) => println(s"${fingerprint} =>> ${value}"))
  }
}
